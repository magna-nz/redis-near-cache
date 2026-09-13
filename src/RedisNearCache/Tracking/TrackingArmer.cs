// ---------------------------------------------------------------------------------------------------
// Manual smoke test (containers from the repo root: `docker compose up -d` and `./cluster-up.sh`)
//
// Standalone (localhost:6379)
//   1. Build a RedisNearCacheConnection for "localhost:6379", start an InvalidationListener, then a
//      TrackingArmer. Expect one Armed(Initial) event for 127.0.0.1:6379 and a Debug line
//      "TRACKINGINFO ... flags=[on] redirect=<id>".
//   2. Confirm the redirect target is really our subscriber connection:
//        docker exec redis-near-cache-redis redis-cli CLIENT LIST | grep rnc-
//      The id in RedirectTargets must be the entry whose flags contain 'P' (pub/sub subscriber).
//   3. Read a key through the private multiplexer (db.StringGetAsync("smoke:k")), then write it from
//      outside:  docker exec redis-near-cache-redis redis-cli SET smoke:k v2
//      InvalidationListener must raise KeyInvalidated("smoke:k") within ~50 ms.
//   4. Interactive reconnect (spike E5): kill our interactive connection
//        docker exec redis-near-cache-redis redis-cli CLIENT KILL ID <interactive id>
//      Expect ConnectionRestored(Interactive) -> Armed(InteractiveRestored) with the SAME redirect id,
//      and invalidations working again after re-reading the key.
//   5. Subscriber reconnect (spike E5b): kill the subscriber connection (the 'P' entry)
//        docker exec redis-near-cache-redis redis-cli CLIENT KILL ID <subscriber id>
//      Expect ConnectionRestored(Subscription) -> Armed(SubscriptionRestored) with a DIFFERENT redirect
//      id than before, and invalidations still arriving afterwards. Without the re-arm the server keeps
//      redirecting to the dead id and this step silently delivers nothing - that is the regression to
//      watch for.
//   6. FLUSHDB from redis-cli must raise FlushAll (null payload).
//   7. Dispose the armer, then `CLIENT TRACKINGINFO` on a fresh admin connection from the same client
//      must show flags=off / redirect=-1 for our (now closed) connection; simplest check is that no
//      further invalidations arrive.
//
// Cluster (127.0.0.1:7100-7102)
//   8. Same setup against "127.0.0.1:7100,127.0.0.1:7101,127.0.0.1:7102". Expect three Armed(Initial)
//      events, one per master, with three DIFFERENT redirect ids (client ids are per node). Verify with
//        for p in 7100 7101 7102; do docker exec redis-near-cache-cluster redis-cli -p $p CLIENT LIST | grep rnc-; done
//      and check each RedirectTargets[ep] against that node's own 'P' entry - never against another node's.
//   9. Pick one key per node (spike ClusterExperiments.ProbeKeysAsync does the slot maths), read each
//      through the cache, then `redis-cli -c -p 7100 SET <key> v2` for each: one KeyInvalidated per key,
//      including for nodes where __redis__:invalidate is not itself subscribed (spike E7a).
//  10. Kill the subscriber connection on ONE node only and confirm exactly one Armed(SubscriptionRestored)
//      for that endpoint and that the other two RedirectTargets entries are unchanged.
// ---------------------------------------------------------------------------------------------------

using System.Collections.Concurrent;
using System.Net;
using Microsoft.Extensions.Logging;
using RedisNearCache.Internal;
using StackExchange.Redis;

namespace RedisNearCache.Tracking;

/// <summary>
/// Owns the <c>CLIENT TRACKING</c> lifecycle on the private multiplexer, one master node at a time.
/// </summary>
/// <remarks>
/// Arming a node means: find this multiplexer's subscriber connection on that node through
/// <c>CLIENT LIST</c> (our client name plus the <see cref="ClientFlags.PubSubSubscriber"/> flag), then
/// <c>CLIENT TRACKING OFF</c> followed by <c>CLIENT TRACKING ON REDIRECT &lt;subscriber id&gt;</c> over that
/// node's interactive connection. Client ids are per node, so the discovery is repeated on every node and a
/// subscriber id is never reused across endpoints.
/// Re-arms happen on <see cref="IConnectionMultiplexer.ConnectionRestored"/> for both connection types: an
/// interactive reconnect makes the server forget tracking, and a subscriber reconnect gives us a new client
/// id while the server keeps redirecting to the dead one (silently losing invalidations).
/// </remarks>
internal sealed class TrackingArmer : ITrackingArmer
{
    /// <summary>Delays between arm attempts. One fewer entry than the number of attempts.</summary>
    private static readonly TimeSpan[] RetryDelays =
    [
        TimeSpan.FromMilliseconds(100),
        TimeSpan.FromMilliseconds(300),
        TimeSpan.FromSeconds(1),
        TimeSpan.FromSeconds(1),
    ];

    private readonly RedisNearCacheConnection _connection;
    private readonly ILogger<TrackingArmer> _logger;
    private readonly ConcurrentDictionary<EndPoint, long> _redirectTargets = new();
    private readonly ConcurrentDictionary<EndPoint, SemaphoreSlim> _gates = new();
    private readonly CancellationTokenSource _shutdown = new();
    private int _started;
    private int _disposed;
    private bool _hooked;

    public TrackingArmer(RedisNearCacheConnection connection, ILogger<TrackingArmer> logger)
    {
        _connection = connection;
        _logger = logger;
    }

    /// <inheritdoc />
    public event Action<TrackingArmedEvent>? Armed;

    /// <inheritdoc />
    public event Action<EndPoint>? TrackingLost;

    /// <summary>
    /// Snapshot of the redirect client id currently believed to be armed on each endpoint. An entry survives a
    /// connection failure (it is the last id we armed) until the endpoint is re-armed; <see cref="TrackingLost"/>
    /// is the signal that an entry is stale.
    /// </summary>
    public IReadOnlyDictionary<EndPoint, long> RedirectTargets => new Dictionary<EndPoint, long>(_redirectTargets);

    /// <inheritdoc />
    public async Task StartAsync(CancellationToken cancellationToken)
    {
        if (Interlocked.Exchange(ref _started, 1) == 1) return;

        HookEvents();
        await RearmAllAsync(ArmReason.Initial, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Re-arms one endpoint now, serialized against any other arm of the same endpoint and retried with
    /// backoff. Throws when every attempt failed, so that callers (tests, diagnostics) see the failure; the
    /// connection-event handlers use the non-throwing path instead and only log.
    /// </summary>
    public async Task RearmAsync(EndPoint endPoint, ArmReason reason, CancellationToken cancellationToken)
    {
        ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) == 1, this);

        using var linked = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, _shutdown.Token);
        var error = await ArmWithRetryAsync(endPoint, reason, linked.Token).ConfigureAwait(false);
        if (error is not null)
            throw new RedisNearCacheTrackingException($"Could not arm CLIENT TRACKING on {endPoint} after {RetryDelays.Length + 1} attempts.", error);
    }

    /// <summary>
    /// Re-arms every connected master. Endpoints are armed concurrently (each has its own gate) and one
    /// failure never cancels the others. A partial failure is logged as a warning; failing on every endpoint
    /// (or finding no connected master at all) throws, because nothing would then be tracked.
    /// </summary>
    public async Task RearmAllAsync(ArmReason reason, CancellationToken cancellationToken)
    {
        ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) == 1, this);

        using var linked = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, _shutdown.Token);
        var endPoints = MasterEndPoints();
        if (endPoints.Count == 0)
        {
            _logger.LogWarning("RedisNearCache found no connected master to arm CLIENT TRACKING on ({Reason})", reason);
            throw new RedisNearCacheTrackingException($"No connected master to arm CLIENT TRACKING on ({reason}).");
        }

        var results = await Task.WhenAll(endPoints.Select(ep => ArmWithRetryAsync(ep, reason, linked.Token))).ConfigureAwait(false);
        var failures = new List<Exception>();
        for (var i = 0; i < endPoints.Count; i++)
        {
            if (results[i] is not { } error) continue;
            failures.Add(error);
            // The endpoint is unarmed: say so (the facade stops caching until it is re-armed) and keep
            // retrying in the background instead of leaving it silently untracked.
            MarkLostAndRetryLater(endPoints[i], reason);
        }
        if (failures.Count == 0) return;

        if (failures.Count == endPoints.Count)
            throw new RedisNearCacheTrackingException($"Could not arm CLIENT TRACKING on any of the {endPoints.Count} connected master(s).", new AggregateException(failures));

        _logger.LogWarning(new AggregateException(failures),
            "RedisNearCache armed CLIENT TRACKING on {Armed} of {Total} master(s) ({Reason}); the cache stays in pass-through until the remaining node(s) are armed",
            endPoints.Count - failures.Count, endPoints.Count, reason);
    }

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) == 1) return;

        UnhookEvents();
        try
        {
            await _shutdown.CancelAsync().ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            _logger.LogDebug(ex, "RedisNearCache ignored an error cancelling the tracking armer");
        }

        foreach (var endPoint in _redirectTargets.Keys.ToArray())
        {
            try
            {
                var server = _connection.Multiplexer.GetServer(endPoint);
                if (!server.IsConnected) continue;
                await server.ExecuteAsync("CLIENT", "TRACKING", "OFF").ConfigureAwait(false);
                _logger.LogDebug("RedisNearCache turned CLIENT TRACKING off on {EndPoint}", endPoint);
            }
            catch (Exception ex)
            {
                _logger.LogDebug(ex, "RedisNearCache could not turn CLIENT TRACKING off on {EndPoint} while disposing", endPoint);
            }
        }

        _redirectTargets.Clear();
        foreach (var gate in _gates.Values) gate.Dispose();
        _gates.Clear();
        _shutdown.Dispose();
    }

    // --- arming ---------------------------------------------------------------------------------------

    /// <summary>
    /// Arms one endpoint, retrying with backoff. Returns null on success, otherwise the last error.
    /// Never throws except for cancellation, so it is safe to call from a connection-event handler.
    /// </summary>
    private async Task<Exception?> ArmWithRetryAsync(EndPoint endPoint, ArmReason reason, CancellationToken cancellationToken)
    {
        // One gate per endpoint: two arms of the same node must never interleave (CLIENT TRACKING OFF from
        // one would undo the ON of the other). Different nodes are independent and run concurrently.
        var gate = _gates.GetOrAdd(endPoint, _ => new SemaphoreSlim(1, 1));
        try
        {
            await gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (ObjectDisposedException ex)
        {
            return ex; // disposed while we were queued: not a success
        }

        try
        {
            Exception? lastError = null;
            for (var attempt = 0; attempt <= RetryDelays.Length; attempt++)
            {
                cancellationToken.ThrowIfCancellationRequested();
                if (attempt > 0)
                {
                    var delay = RetryDelays[attempt - 1];
                    _logger.LogWarning(lastError,
                        "RedisNearCache retrying CLIENT TRACKING arm on {EndPoint} ({Reason}) in {Delay} ms, attempt {Attempt} of {Attempts}",
                        endPoint, reason, delay.TotalMilliseconds, attempt + 1, RetryDelays.Length + 1);
                    await Task.Delay(delay, cancellationToken).ConfigureAwait(false);
                }

                try
                {
                    if (await TryArmAsync(endPoint, reason, cancellationToken).ConfigureAwait(false)) return null;
                    lastError = null;
                }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                {
                    throw;
                }
                catch (Exception ex)
                {
                    lastError = ex;
                }
            }

            _logger.LogWarning(lastError,
                "RedisNearCache gave up arming CLIENT TRACKING on {EndPoint} ({Reason}) after {Attempts} attempts; invalidations from this node will not be received until it is re-armed",
                endPoint, reason, RetryDelays.Length + 1);
            return lastError ?? new RedisNearCacheTrackingException($"No subscriber connection of client {_connection.ClientName} was found on {endPoint}.");
        }
        finally
        {
            try { gate.Release(); } catch (ObjectDisposedException) { /* disposed underneath us */ }
        }
    }

    /// <summary>
    /// One arm attempt. Returns false (without throwing) when the node or its subscriber connection is not
    /// there yet, which is the normal state immediately after a reconnect event.
    /// </summary>
    private async Task<bool> TryArmAsync(EndPoint endPoint, ArmReason reason, CancellationToken cancellationToken)
    {
        if (Volatile.Read(ref _disposed) == 1) return true;

        var server = _connection.Multiplexer.GetServer(endPoint);
        if (!server.IsConnected)
        {
            _logger.LogDebug("RedisNearCache cannot arm {EndPoint} yet: the interactive connection is not connected", endPoint);
            return false;
        }

        if (server.IsReplica)
        {
            _logger.LogDebug("RedisNearCache skipped arming {EndPoint}: it is a replica", endPoint);
            return true;
        }

        var clients = await server.ClientListAsync().WaitAsync(cancellationToken).ConfigureAwait(false);
        var subscriberId = FindSubscriberId(clients, endPoint);
        if (subscriberId is not { } redirectId)
        {
            _logger.LogDebug("RedisNearCache found no subscriber connection for client {ClientName} on {EndPoint} yet", _connection.ClientName, endPoint);
            return false;
        }

        await server.ExecuteAsync("CLIENT", "TRACKING", "OFF").WaitAsync(cancellationToken).ConfigureAwait(false);
        // NOLOOP: our own writes (SetAsync/RemoveAsync on this connection) evict L1 synchronously and mark the
        // key in the in-flight tracker, so the server echoing an invalidation back would only race the next read.
        await server.ExecuteAsync("CLIENT", "TRACKING", "ON", "REDIRECT", redirectId, "NOLOOP").WaitAsync(cancellationToken).ConfigureAwait(false);

        if (!await VerifyAsync(server, endPoint, redirectId, cancellationToken).ConfigureAwait(false)) return false;

        _redirectTargets[endPoint] = redirectId;
        _logger.LogInformation("RedisNearCache armed CLIENT TRACKING on {EndPoint} redirecting to client {RedirectClientId} ({Reason})",
            endPoint, redirectId, reason);
        Raise(Armed, new TrackingArmedEvent(endPoint, redirectId, reason), nameof(Armed));
        return true;
    }

    /// <summary>
    /// Picks this multiplexer's subscriber connection on one node: our client name plus the pub/sub flag.
    /// If a dying connection is still listed alongside its replacement, the highest id is the new one.
    /// </summary>
    private long? FindSubscriberId(ClientInfo[]? clients, EndPoint endPoint)
    {
        if (clients is null || clients.Length == 0) return null;

        long? best = null;
        var matches = 0;
        foreach (var client in clients)
        {
            if (!string.Equals(client.Name, _connection.ClientName, StringComparison.Ordinal)) continue;
            if ((client.Flags & ClientFlags.PubSubSubscriber) == 0) continue;
            matches++;
            if (best is null || client.Id > best.Value) best = client.Id;
        }

        if (matches > 1)
            _logger.LogWarning("RedisNearCache saw {Count} subscriber connections named {ClientName} on {EndPoint}; redirecting to the newest ({RedirectClientId})",
                matches, _connection.ClientName, endPoint, best);

        return best;
    }

    /// <summary>Reads CLIENT TRACKINGINFO back over the same interactive connection and checks the redirect id.</summary>
    private async Task<bool> VerifyAsync(IServer server, EndPoint endPoint, long expectedRedirectId, CancellationToken cancellationToken)
    {
        RedisResult info;
        try
        {
            info = await server.ExecuteAsync("CLIENT", "TRACKINGINFO").WaitAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            _logger.LogDebug(ex, "RedisNearCache could not read CLIENT TRACKINGINFO on {EndPoint}; assuming the arm took", endPoint);
            return true;
        }

        _logger.LogDebug("RedisNearCache CLIENT TRACKINGINFO on {EndPoint} => {TrackingInfo}", endPoint, Describe(info));

        if (!TryReadRedirect(info, out var actualRedirectId)) return true; // unexpected shape: do not fight the server
        if (actualRedirectId == expectedRedirectId) return true;

        _logger.LogWarning("RedisNearCache armed {EndPoint} with REDIRECT {Expected} but CLIENT TRACKINGINFO reports {Actual}; retrying",
            endPoint, expectedRedirectId, actualRedirectId);
        return false;
    }

    // --- multiplexer events ---------------------------------------------------------------------------

    private void HookEvents()
    {
        if (_hooked) return;
        _hooked = true;
        var mux = _connection.Multiplexer;
        mux.ConnectionRestored += OnConnectionRestored;
        mux.ConnectionFailed += OnConnectionFailed;
        mux.ConfigurationChanged += OnConfigurationChanged;
        mux.ConfigurationChangedBroadcast += OnConfigurationChanged;
    }

    private void UnhookEvents()
    {
        if (!_hooked) return;
        _hooked = false;
        var mux = _connection.Multiplexer;
        mux.ConnectionRestored -= OnConnectionRestored;
        mux.ConnectionFailed -= OnConnectionFailed;
        mux.ConfigurationChanged -= OnConfigurationChanged;
        mux.ConfigurationChangedBroadcast -= OnConfigurationChanged;
    }

    private void OnConnectionRestored(object? sender, ConnectionFailedEventArgs e)
    {
        try
        {
            if (Volatile.Read(ref _disposed) == 1 || e.EndPoint is not { } endPoint) return;

            // Interactive reconnect: the server dropped tracking entirely (TRACKINGINFO flags=off, redirect=-1).
            // Subscription reconnect: the subscriber came back with a NEW client id and the server is still
            // redirecting to the dead one, which loses invalidations silently. Both need a full re-arm.
            ArmReason reason;
            switch (e.ConnectionType)
            {
                case ConnectionType.Interactive:
                    reason = ArmReason.InteractiveRestored;
                    break;
                case ConnectionType.Subscription:
                    reason = ArmReason.SubscriptionRestored;
                    break;
                default:
                    return;
            }

            _logger.LogInformation("RedisNearCache saw {ConnectionType} restored on {EndPoint}; re-arming CLIENT TRACKING", e.ConnectionType, endPoint);
            QueueArm(endPoint, reason);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "RedisNearCache failed to handle ConnectionRestored");
        }
    }

    private void OnConnectionFailed(object? sender, ConnectionFailedEventArgs e)
    {
        try
        {
            if (Volatile.Read(ref _disposed) == 1 || e.EndPoint is not { } endPoint) return;
            if (e.ConnectionType is not (ConnectionType.Interactive or ConnectionType.Subscription)) return;

            _logger.LogWarning(e.Exception, "RedisNearCache lost the {ConnectionType} connection to {EndPoint} ({FailureType}); tracking is not active until it is re-armed",
                e.ConnectionType, endPoint, e.FailureType);
            Raise(TrackingLost, endPoint, nameof(TrackingLost));
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "RedisNearCache failed to handle ConnectionFailed");
        }
    }

    /// <summary>Cluster topology changed: arm any master we have never armed. Known endpoints are left alone.</summary>
    private void OnConfigurationChanged(object? sender, EndPointEventArgs e)
    {
        try
        {
            if (Volatile.Read(ref _disposed) == 1) return;

            foreach (var endPoint in MasterEndPoints())
            {
                if (_redirectTargets.ContainsKey(endPoint)) continue;
                _logger.LogInformation("RedisNearCache saw a configuration change adding master {EndPoint}; arming CLIENT TRACKING", endPoint);
                QueueArm(endPoint, ArmReason.TopologyChanged);
            }
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "RedisNearCache failed to handle a configuration change");
        }
    }

    /// <summary>Runs an arm off the event-handler thread. Swallows everything; callers are event handlers.</summary>
    private void QueueArm(EndPoint endPoint, ArmReason reason)
    {
        CancellationToken token;
        try
        {
            token = _shutdown.Token;
        }
        catch (ObjectDisposedException)
        {
            return;
        }

        _ = Task.Run(async () =>
        {
            try
            {
                var error = await ArmWithRetryAsync(endPoint, reason, token).ConfigureAwait(false);
                if (error is not null) MarkLostAndRetryLater(endPoint, reason);
            }
            catch (OperationCanceledException)
            {
                // shutting down
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "RedisNearCache failed to re-arm CLIENT TRACKING on {EndPoint} ({Reason})", endPoint, reason);
            }
        }, CancellationToken.None);
    }

    /// <summary>Interval between background retries once the fast backoff ladder has been exhausted.</summary>
    private static readonly TimeSpan SlowRetryInterval = TimeSpan.FromSeconds(5);

    /// <summary>
    /// An arm exhausted its retries. Forget the (dead) redirect id so the endpoint is not mistaken for armed,
    /// raise <see cref="TrackingLost"/> so the facade stops caching, and keep retrying every
    /// <see cref="SlowRetryInterval"/> until it succeeds or we are disposed.
    /// </summary>
    private void MarkLostAndRetryLater(EndPoint endPoint, ArmReason reason)
    {
        _redirectTargets.TryRemove(endPoint, out _);
        Raise(TrackingLost, endPoint, nameof(TrackingLost));

        CancellationToken token;
        try { token = _shutdown.Token; } catch (ObjectDisposedException) { return; }

        _ = Task.Run(async () =>
        {
            try
            {
                while (!token.IsCancellationRequested)
                {
                    await Task.Delay(SlowRetryInterval, token).ConfigureAwait(false);
                    _logger.LogInformation("RedisNearCache retrying CLIENT TRACKING arm on {EndPoint} ({Reason}) after the backoff ladder was exhausted", endPoint, reason);
                    if (await ArmWithRetryAsync(endPoint, reason, token).ConfigureAwait(false) is null) return;
                }
            }
            catch (OperationCanceledException)
            {
                // shutting down
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "RedisNearCache background re-arm of {EndPoint} stopped unexpectedly", endPoint);
            }
        }, CancellationToken.None);
    }

    // --- helpers --------------------------------------------------------------------------------------

    private List<EndPoint> MasterEndPoints()
    {
        var endPoints = new List<EndPoint>();
        try
        {
            foreach (var server in _connection.ConnectedMasters())
            {
                if (server.EndPoint is { } endPoint) endPoints.Add(endPoint);
            }
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "RedisNearCache could not enumerate connected masters");
        }

        return endPoints;
    }

    private void Raise<T>(Action<T>? handler, T argument, string name)
    {
        if (handler is null) return;
        try
        {
            handler(argument);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "RedisNearCache {EventName} handler threw", name);
        }
    }

    /// <summary>Reads the <c>redirect</c> field out of a CLIENT TRACKINGINFO reply (a flat RESP2 key/value array).</summary>
    private static bool TryReadRedirect(RedisResult info, out long redirect)
    {
        redirect = -1;
        try
        {
            if (info.IsNull) return false;
            var items = (RedisResult[]?)info;
            if (items is null) return false;

            for (var i = 0; i + 1 < items.Length; i += 2)
            {
                if (!string.Equals(items[i].ToString(), "redirect", StringComparison.OrdinalIgnoreCase)) continue;
                redirect = (long)items[i + 1];
                return true;
            }
        }
        catch (InvalidCastException)
        {
            return false;
        }

        return false;
    }

    /// <summary>Flattens a RedisResult for a log line.</summary>
    private static string Describe(RedisResult result)
    {
        if (result.IsNull) return "<null>";
        if (result.Resp2Type == ResultType.Array)
        {
            var items = (RedisResult[]?)result;
            if (items is not null) return "[" + string.Join(" ", items.Select(Describe)) + "]";
        }

        return result.ToString() ?? "<null>";
    }
}

/// <summary>Raised when CLIENT TRACKING could not be armed on an endpoint.</summary>
internal sealed class RedisNearCacheTrackingException : Exception
{
    public RedisNearCacheTrackingException(string message) : base(message) { }

    public RedisNearCacheTrackingException(string message, Exception? innerException) : base(message, innerException) { }
}
