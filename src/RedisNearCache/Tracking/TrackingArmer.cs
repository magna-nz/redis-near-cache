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
    private static readonly TimeSpan[] RetryDelays = TrackingRetry.Delays;

    private readonly RedisNearCacheConnection _connection;
    private readonly ILogger<TrackingArmer> _logger;
    private readonly ConcurrentDictionary<EndPoint, long> _redirectTargets = new();
    private readonly ConcurrentDictionary<EndPoint, SemaphoreSlim> _gates = new();
    private readonly CancellationTokenSource _shutdown = new();
    private readonly TimeSpan _reconcileInterval;
    private int _started;
    private int _disposed;
    private bool _hooked;
    private int _reconciling;
    private int _preArmPending;

    /// <summary>
    /// Replicas armed ahead of time (see <see cref="PreArmReplicasAsync"/>), keyed by endpoint with the redirect id
    /// in use there. An entry is dropped as soon as a connection to the replica fails, because the server then
    /// dropped the tracking with it (interactive) or the redirect id is dead (subscriber).
    /// </summary>
    private readonly ConcurrentDictionary<EndPoint, long> _replicaTargets = new();

    /// <summary>
    /// Per endpoint, bumped under <see cref="_lifecycle"/> on every connection failure. A replica pre-arm commits
    /// only if the epoch it started under is still current, so an arm whose connection dropped while its last
    /// reply was in flight is never recorded (the server dropped the tracking with the connection).
    /// </summary>
    private readonly ConcurrentDictionary<EndPoint, long> _connectionEpochs = new();

    /// <summary>
    /// The interval of the background reconcile loop, which arms replicas ahead of promotion and re-reads the
    /// multiplexer's topology. Tests pass <see cref="Timeout.InfiniteTimeSpan"/> to drive every step by hand.
    /// </summary>
    public static readonly TimeSpan DefaultReconcileInterval = TimeSpan.FromSeconds(5);

    public TrackingArmer(RedisNearCacheConnection connection, ILogger<TrackingArmer> logger, TimeSpan? reconcileInterval = null)
    {
        _connection = connection;
        _logger = logger;
        _reconcileInterval = reconcileInterval ?? DefaultReconcileInterval;
    }

    /// <inheritdoc />
    public event Action<TrackingArmedEvent>? Armed;

    /// <inheritdoc />
    public event Action<EndPoint>? TrackingLost;

    /// <inheritdoc />
    public event Action<EndPoint>? EndpointRemoved;

    /// <summary>
    /// Endpoints that currently have a background slow-retry loop; at most one loop per endpoint. The value is the
    /// owning loop's marker, so a loop that is exiting never removes the entry of a loop started after it.
    /// </summary>
    private readonly ConcurrentDictionary<EndPoint, object> _retrying = new();

    /// <summary>
    /// Endpoints for which <see cref="TrackingLost"/> was the last lifecycle event raised, i.e. the facade is in
    /// pass-through waiting on them. Every such endpoint must eventually get <see cref="Armed"/> or
    /// <see cref="EndpointRemoved"/>. Mutated and raised together under <see cref="_lifecycle"/>, so the order the
    /// facade sees events in always matches this set, even when two threads raise for the same endpoint.
    /// </summary>
    private readonly ConcurrentDictionary<EndPoint, byte> _lost = new();

    private readonly object _lifecycle = new();

    /// <summary>
    /// Snapshot of the redirect client id currently believed to be armed on each endpoint. An entry survives a
    /// connection failure (it is the last id we armed) until the endpoint is re-armed; <see cref="TrackingLost"/>
    /// is the signal that an entry is stale.
    /// </summary>
    public IReadOnlyDictionary<EndPoint, long> RedirectTargets => new Dictionary<EndPoint, long>(_redirectTargets);

    /// <inheritdoc />
    public IReadOnlyDictionary<EndPoint, long> ReplicaRedirectTargets => new Dictionary<EndPoint, long>(_replicaTargets);

    /// <inheritdoc />
    public async Task StartAsync(CancellationToken cancellationToken)
    {
        if (Interlocked.Exchange(ref _started, 1) == 1) return;

        HookEvents();
        await RearmAllAsync(ArmReason.Initial, cancellationToken).ConfigureAwait(false);
        // Replicas are armed after Ready, never as part of it: a failover cannot be made to wait on them and a
        // replica that cannot be armed costs nothing but the pre-arm optimisation.
        QueuePreArmReplicas();
        StartReconcileLoop();
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

        foreach (var endPoint in _replicaTargets.Keys.ToArray())
        {
            try
            {
                var server = _connection.Multiplexer.GetServer(endPoint);
                if (!server.IsConnected) continue;
                await server.ExecuteAsync("CLIENT", "TRACKING", "OFF").ConfigureAwait(false);
                _logger.LogDebug("RedisNearCache turned CLIENT TRACKING off on pre-armed replica {EndPoint}", endPoint);
            }
            catch (Exception ex)
            {
                _logger.LogDebug(ex, "RedisNearCache could not turn CLIENT TRACKING off on replica {EndPoint} while disposing", endPoint);
            }
        }

        _redirectTargets.Clear();
        _replicaTargets.Clear();
        _lost.Clear();
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
        // Any arm after the initial pass means tracking on this node is, or is about to be, unreliable (OFF is issued
        // before ON). Say so first, BEFORE waiting for the gate: a replica pre-arm may be holding it for a few round
        // trips, and reads routed to this node meanwhile must not be stored. The facade then stays in pass-through
        // for the whole attempt, including the backoff ladder and a failed TRACKINGINFO verification.
        if (reason != ArmReason.Initial) MarkLost(endPoint, forgetRedirect: false);

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
                if (Volatile.Read(ref _disposed) == 1) return new ObjectDisposedException(nameof(TrackingArmer));
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
                catch (RedisNearCacheTrackingException ex)
                {
                    // The server rejected the command itself (not a transient state): the ladder cannot help.
                    _logger.LogWarning(ex, "RedisNearCache cannot arm CLIENT TRACKING on {EndPoint} ({Reason}); giving up the fast retries", endPoint, reason);
                    return ex;
                }
                catch (Exception ex)
                {
                    lastError = ex;
                }
            }

            _logger.LogWarning(lastError,
                "RedisNearCache gave up arming CLIENT TRACKING on {EndPoint} ({Reason}) after {Attempts} attempts; invalidations from this node will not be received until it is re-armed",
                endPoint, reason, RetryDelays.Length + 1);
            if (lastError is RedisServerException rejected)
            {
                // The server refused a command the redirect design needs (CLIENT LIST, CLIENT TRACKING OFF, ...):
                // the usual reason is a proxy, so say what to do rather than hand back the bare server error.
                return new RedisNearCacheTrackingException(
                    $"{endPoint} rejected a command RedisNearCache needs for REDIRECT tracking: {rejected.Message}. If this is a Redis " +
                    "Enterprise-based service (Azure Managed Redis, Redis Cloud, Redis Software), set " +
                    "RedisNearCacheOptions.TrackingMode = TrackingMode.Broadcast.", rejected);
            }

            return lastError ?? new RedisNearCacheTrackingException(
                $"No subscriber connection of client {_connection.ClientName} was found on {endPoint}. If this is a Redis " +
                "Enterprise-based service (Azure Managed Redis, Redis Cloud, Redis Software), its proxy hides that connection " +
                "from CLIENT LIST and rejects REDIRECT: set RedisNearCacheOptions.TrackingMode = TrackingMode.Broadcast.");
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
        if (Volatile.Read(ref _disposed) == 1) throw new ObjectDisposedException(nameof(TrackingArmer));

        var server = _connection.Multiplexer.GetServer(endPoint);
        if (!server.IsConnected)
        {
            _logger.LogDebug("RedisNearCache cannot arm {EndPoint} yet: the interactive connection is not connected", endPoint);
            return false;
        }

        if (server.IsReplica)
        {
            // Nothing to arm on a replica. If we had armed it, or told the facade it was lost (every non-initial
            // arm does), it was a master and has been demoted: forget it, which flushes L1 and releases the facade
            // from pass-through. Returning without an event would leave the facade waiting for it forever.
            if (_redirectTargets.ContainsKey(endPoint) || _lost.ContainsKey(endPoint))
            {
                _logger.LogInformation("RedisNearCache found {EndPoint} is a replica now ({Reason}); forgetting it", endPoint, reason);
                RemoveEndpoint(endPoint);
            }
            else
            {
                _logger.LogDebug("RedisNearCache skipped arming {EndPoint}: it is a replica", endPoint);
            }
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
        await TrackingOnAsync(server, endPoint, redirectId, cancellationToken).ConfigureAwait(false);

        if (!await VerifyAsync(server, endPoint, redirectId, cancellationToken).ConfigureAwait(false)) return false;

        _logger.LogInformation("RedisNearCache armed CLIENT TRACKING on {EndPoint} redirecting to client {RedirectClientId} ({Reason})",
            endPoint, redirectId, reason);
        lock (_lifecycle)
        {
            _redirectTargets[endPoint] = redirectId;
            _replicaTargets.TryRemove(endPoint, out _); // an armed master is never also a pre-armed replica
            _lost.TryRemove(endPoint, out _);
            Raise(Armed, new TrackingArmedEvent(endPoint, redirectId, reason), nameof(Armed));
        }
        return true;
    }

    /// <summary>
    /// <c>CLIENT TRACKING ON REDIRECT &lt;id&gt; OPTOUT NOLOOP</c>. OPTOUT: every read is tracked unless the facade
    /// precedes it with <c>CLIENT CACHING NO</c>, which it does for keys outside <see cref="RedisNearCacheOptions.KeyPrefixes"/>.
    /// NOLOOP: our own writes (SetAsync/RemoveAsync on this connection) evict L1 synchronously and mark the key in
    /// the in-flight tracker, so the server echoing an invalidation back would only race the next read.
    /// </summary>
    private static async Task TrackingOnAsync(IServer server, EndPoint endPoint, long redirectId, CancellationToken cancellationToken)
    {
        try
        {
            await server.ExecuteAsync("CLIENT", "TRACKING", "ON", "REDIRECT", redirectId, "OPTOUT", "NOLOOP").WaitAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (RedisServerException ex) when (IsUnsupportedCommandError(ex.Message))
        {
            // Not a transient failure: the fast retry ladder is skipped for this exception type (the slow 5 s loop
            // still runs, in case the endpoint changes underneath us). The usual cause is a managed service whose
            // proxy does not implement two-connection tracking at all.
            throw new RedisNearCacheTrackingException(
                $"{endPoint} rejected CLIENT TRACKING ON REDIRECT: {ex.Message}. RedisNearCache needs RESP2 two-connection " +
                "tracking (REDIRECT), which Redis Enterprise-based services (Azure Managed Redis, Redis Cloud, Redis Software) " +
                "and ElastiCache Serverless do not support. For the Enterprise-based services set " +
                "RedisNearCacheOptions.TrackingMode = TrackingMode.Broadcast.", ex);
        }
    }

    /// <summary>
    /// True for the errors a server or proxy returns when it does not implement the command or one of its options
    /// (Redis: "ERR unknown subcommand", "ERR syntax error"; proxies: "unsupported", "not allowed"), as opposed to
    /// transient states such as LOADING or a MASTERDOWN that a retry can outlast. An ACL denial (NOPERM) is
    /// deliberately not matched: that is the caller's configuration, not the service.
    /// </summary>
    private static bool IsUnsupportedCommandError(string message) =>
        message.Contains("unknown", StringComparison.OrdinalIgnoreCase)
        || message.Contains("syntax", StringComparison.OrdinalIgnoreCase)
        || message.Contains("unsupported", StringComparison.OrdinalIgnoreCase)
        || message.Contains("not supported", StringComparison.OrdinalIgnoreCase)
        || message.Contains("not allowed", StringComparison.OrdinalIgnoreCase);

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
            if (!IsTrackedMaster(endPoint)) return;

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
            // A pre-armed replica whose connection dropped is no longer known to be armed: the interactive reconnect
            // loses the server's tracking state, the subscriber reconnect gets a new client id. Forget it; the
            // reconcile loop re-arms it once the multiplexer confirms it is (still) a replica. If it is promoted
            // before then, the promotion takes the ordinary arm-and-flush path.
            bool wasPreArmed;
            lock (_lifecycle)
            {
                _connectionEpochs.AddOrUpdate(endPoint, 1, static (_, epoch) => epoch + 1);
                wasPreArmed = _replicaTargets.TryRemove(endPoint, out _);
            }
            if (wasPreArmed)
                _logger.LogDebug("RedisNearCache lost the {ConnectionType} connection to pre-armed replica {EndPoint}; it will be re-armed", e.ConnectionType, endPoint);
            if (!IsTrackedMaster(endPoint))
            {
                _logger.LogDebug("RedisNearCache ignored a {ConnectionType} failure on {EndPoint}: not a master we track", e.ConnectionType, endPoint);
                return;
            }

            _logger.LogWarning(e.Exception, "RedisNearCache lost the {ConnectionType} connection to {EndPoint} ({FailureType}); tracking is not active until it is re-armed",
                e.ConnectionType, endPoint, e.FailureType);
            // The server dropped tracking with the connection: forget the redirect id so nothing mistakes
            // this node for armed. ConnectionRestored normally re-arms it; the slow loop below is the
            // safety net for a node that never comes back (it raises EndpointRemoved once the node is no
            // longer a master) and exits quietly if the endpoint was armed or forgotten first.
            MarkLost(endPoint, forgetRedirect: true);
            RetryLater(endPoint, ArmReason.Recovered);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "RedisNearCache failed to handle ConnectionFailed");
        }
    }

    /// <summary>
    /// Topology changed: arm any connected master we have not armed, and forget every endpoint we armed or lost
    /// that is no longer a master (demoted to replica, dropped by the multiplexer, or - outside a cluster -
    /// disconnected while another master is connected). A disconnected cluster master needs the slot map, which
    /// is a network call, so it is left to the background retry loop.
    /// </summary>
    private void OnConfigurationChanged(object? sender, EndPointEventArgs e) => Reconcile("configuration change");

    /// <summary>
    /// Brings the armer in line with the multiplexer's current view of the deployment. Runs on every
    /// <c>ConfigurationChanged</c> and on the reconcile loop's timer, because StackExchange.Redis raises the event
    /// only for a reconfiguration it can blame on an endpoint; a periodic topology check that quietly relearns a
    /// promoted node's role raises nothing. Idempotent, and cheap when nothing changed.
    /// </summary>
    private void Reconcile(string cause)
    {
        try
        {
            if (Volatile.Read(ref _disposed) == 1) return;

            var masters = MasterEndPoints();
            foreach (var endPoint in masters)
            {
                if (_redirectTargets.ContainsKey(endPoint)) continue;
                var promoted = false;
                lock (_lifecycle)
                {
                    // Under the lock, so a connection failure cannot slip between "was pre-armed" and "is an armed
                    // master": OnConnectionFailed removes the pre-arm and bumps the epoch under the same lock, and
                    // once the endpoint is in _redirectTargets a failure takes the ordinary TrackingLost path.
                    if (_replicaTargets.TryRemove(endPoint, out var redirectId))
                    {
                        // Pre-armed while it was a replica, and no connection to it has failed since: tracking there has
                        // been on since before the first read could be routed to it, so it becomes an armed master without
                        // an OFF/ON (which would forget those reads) and without a pass-through gap.
                        _logger.LogInformation("RedisNearCache saw {EndPoint} promoted to master ({Cause}); it was pre-armed as a replica (redirect {RedirectClientId}), so no re-arm is needed",
                            endPoint, cause, redirectId);
                        _redirectTargets[endPoint] = redirectId;
                        _lost.TryRemove(endPoint, out _);
                        Raise(Armed, new TrackingArmedEvent(endPoint, redirectId, ArmReason.Promoted), nameof(Armed));
                        promoted = true;
                    }
                }
                if (promoted) continue;

                _logger.LogInformation("RedisNearCache saw a {Cause} adding master {EndPoint}; arming CLIENT TRACKING", cause, endPoint);
                QueueArm(endPoint, ArmReason.TopologyChanged);
            }

            var candidates = _redirectTargets.Keys.Concat(_lost.Keys).Distinct().Where(ep => !masters.Contains(ep)).ToArray();
            if (candidates.Length > 0)
            {
                var servers = ServerViews();
                // No view of the deployment: never forget endpoints on the strength of nothing.
                if (servers.Count > 0)
                {
                    foreach (var endPoint in candidates)
                    {
                        if (MasterRole.FromMultiplexer(endPoint, servers) is not false) continue;
                        _logger.LogInformation("RedisNearCache saw a {Cause} after which {EndPoint} is no longer a master of this deployment", cause, endPoint);
                        RemoveEndpoint(endPoint);
                    }
                }
            }

            QueuePreArmReplicas();
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "RedisNearCache failed to handle a {Cause}", cause);
        }
    }

    /// <summary>Every <see cref="_reconcileInterval"/>: <see cref="Reconcile"/>, which includes re-arming replicas that lost their pre-arm.</summary>
    private void StartReconcileLoop()
    {
        if (_reconcileInterval == Timeout.InfiniteTimeSpan) return;
        CancellationToken token;
        try { token = _shutdown.Token; } catch (ObjectDisposedException) { return; }

        _ = Task.Run(async () =>
        {
            try
            {
                while (!token.IsCancellationRequested)
                {
                    await Task.Delay(_reconcileInterval, token).ConfigureAwait(false);
                    Reconcile("topology check");
                }
            }
            catch (OperationCanceledException)
            {
                // shutting down
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "RedisNearCache reconcile loop stopped unexpectedly");
            }
        }, CancellationToken.None);
    }

    // --- replica pre-arm --------------------------------------------------------------------------------

    /// <summary>
    /// Runs <see cref="PreArmReplicasAsync"/> in the background, at most one sweep at a time. A request that arrives
    /// while a sweep is running is not dropped (the state it reacts to may have changed after that sweep looked):
    /// it leaves <see cref="_preArmPending"/> set, and the running sweep's owner runs one more sweep for all such
    /// requests once it finishes.
    /// </summary>
    private void QueuePreArmReplicas()
    {
        Volatile.Write(ref _preArmPending, 1);
        RunPendingPreArmSweeps();
    }

    private void RunPendingPreArmSweeps()
    {
        if (Interlocked.CompareExchange(ref _reconciling, 1, 0) != 0) return;
        CancellationToken token;
        try { token = _shutdown.Token; } catch (ObjectDisposedException) { Volatile.Write(ref _reconciling, 0); return; }

        _ = Task.Run(async () =>
        {
            try
            {
                while (!token.IsCancellationRequested && Interlocked.Exchange(ref _preArmPending, 0) == 1)
                {
                    try
                    {
                        await PreArmReplicasAsync(token).ConfigureAwait(false);
                    }
                    catch (OperationCanceledException) when (token.IsCancellationRequested)
                    {
                        return; // shutting down
                    }
                    catch (Exception ex)
                    {
                        _logger.LogDebug(ex, "RedisNearCache replica pre-arm sweep failed");
                    }
                }
            }
            finally
            {
                Volatile.Write(ref _reconciling, 0);
            }

            // A request that set the flag after the loop's last check but before _reconciling was cleared saw a sweep
            // still running and returned; pick it up here.
            if (!token.IsCancellationRequested && Volatile.Read(ref _preArmPending) == 1) RunPendingPreArmSweeps();
        }, CancellationToken.None);
    }

    /// <summary>
    /// Arms every connected replica that is not already armed, and disarms a pre-armed replica whose replication link
    /// is no longer up (see <see cref="PreArmReplicaAsync"/> for why). A replica is never read from, so its tracking
    /// table stays empty until the moment it is promoted; from then on every read routed to it is tracked, which
    /// closes the window between a failover and the next topology check during which a promoted master used to
    /// serve untracked reads. Replicas are handled concurrently (each has its own gate); failures are logged at
    /// debug and retried by the next sweep.
    /// </summary>
    private async Task PreArmReplicasAsync(CancellationToken cancellationToken)
    {
        IServer[] servers;
        try
        {
            servers = _connection.Multiplexer.GetServers();
        }
        catch (Exception ex)
        {
            _logger.LogDebug(ex, "RedisNearCache could not enumerate servers to pre-arm replicas");
            return;
        }

        var work = new List<Task>();
        foreach (var server in servers)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (server.EndPoint is not { } endPoint) continue;
            if (!server.IsConnected || !server.IsReplica || server.ServerType == ServerType.Sentinel) continue;
            if (_redirectTargets.ContainsKey(endPoint) || _lost.ContainsKey(endPoint)) continue;
            work.Add(_replicaTargets.ContainsKey(endPoint)
                ? RecheckReplicaAsync(server, endPoint, cancellationToken)
                : PreArmReplicaAsync(server, endPoint, cancellationToken));
        }

        await Task.WhenAll(work).ConfigureAwait(false);
    }

    /// <summary>
    /// A pre-armed replica whose replication link has gone down is about to (or may already) resynchronise, and a
    /// full resync empties its keyspace, which the server reports to every tracking client as the null
    /// invalidation a FLUSHDB sends. Turn tracking off there and forget the pre-arm; the next sweep re-arms it once
    /// the link is back up. Safe if the node was promoted meanwhile: it is then not in _replicaTargets any more, and
    /// the ordinary master path arms it with a flush.
    /// </summary>
    private async Task RecheckReplicaAsync(IServer server, EndPoint endPoint, CancellationToken cancellationToken)
    {
        try
        {
            var role = await server.ExecuteAsync("ROLE").WaitAsync(cancellationToken).ConfigureAwait(false);
            if (IsReplicaRole(role, requireConnected: true)) return;
            // Not a replica any more: it was promoted and the multiplexer has not noticed yet. Its pre-arm is exactly
            // what the promotion path needs, so leave it (and its tracking) alone.
            if (!IsReplicaRole(role)) return;

            // The OFF goes out under the endpoint's gate, like every other tracking command, so it can never land after
            // a master arm's ON; and only while the endpoint is still nothing but a pre-armed replica.
            var gate = _gates.GetOrAdd(endPoint, _ => new SemaphoreSlim(1, 1));
            await gate.WaitAsync(cancellationToken).ConfigureAwait(false);
            try
            {
                bool dropped;
                lock (_lifecycle)
                {
                    dropped = !_redirectTargets.ContainsKey(endPoint) && !_lost.ContainsKey(endPoint) && _replicaTargets.TryRemove(endPoint, out _);
                }
                if (!dropped) return;
                _logger.LogInformation("RedisNearCache disarmed pre-armed replica {EndPoint}: ROLE {Role}; it will be re-armed once its replication link is up", endPoint, Describe(role));
                await server.ExecuteAsync("CLIENT", "TRACKING", "OFF").WaitAsync(cancellationToken).ConfigureAwait(false);
            }
            finally
            {
                try { gate.Release(); } catch (ObjectDisposedException) { /* disposed underneath us */ }
            }
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            _logger.LogDebug(ex, "RedisNearCache could not re-check pre-armed replica {EndPoint}", endPoint);
        }
    }

    /// <summary>
    /// One replica. Only a replica whose replication link is up and in sync is armed: a replica that is still
    /// loading its master's dataset will empty its keyspace when the transfer completes, and that flush is pushed to
    /// every tracking client on it as the same null invalidation a FLUSHDB sends, which would flush L1 for nothing.
    /// Then <c>CLIENT TRACKING ON</c> is issued without a preceding <c>OFF</c> (ON re-issued on a tracked connection
    /// just updates the redirect and keeps the remembered keys) and is immediately followed by <c>ROLE</c> on the
    /// same connection. Commands on one connection execute in order, so if ROLE still says replica then tracking was
    /// on before any read could have been routed here as a master; if it says master, the promotion may have
    /// preceded the ON and the node is left to the ordinary arm-and-flush path.
    /// </summary>
    private async Task PreArmReplicaAsync(IServer server, EndPoint endPoint, CancellationToken cancellationToken)
    {
        try
        {
            await PreArmReplicaCoreAsync(server, endPoint, cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            _logger.LogDebug(ex, "RedisNearCache could not pre-arm replica {EndPoint}; it will be retried", endPoint);
        }
    }

    private async Task PreArmReplicaCoreAsync(IServer server, EndPoint endPoint, CancellationToken cancellationToken)
    {
        if (Volatile.Read(ref _disposed) == 1) return;
        var gate = _gates.GetOrAdd(endPoint, _ => new SemaphoreSlim(1, 1));
        await gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (_replicaTargets.ContainsKey(endPoint) || _redirectTargets.ContainsKey(endPoint) || _lost.ContainsKey(endPoint)) return;
            long epoch;
            lock (_lifecycle) epoch = _connectionEpochs.GetValueOrDefault(endPoint);

            var before = await server.ExecuteAsync("ROLE").WaitAsync(cancellationToken).ConfigureAwait(false);
            if (!IsReplicaRole(before, requireConnected: true))
            {
                _logger.LogDebug("RedisNearCache is not pre-arming {EndPoint} yet: ROLE {Role}", endPoint, Describe(before));
                return;
            }

            var clients = await server.ClientListAsync().WaitAsync(cancellationToken).ConfigureAwait(false);
            if (FindSubscriberId(clients, endPoint) is not { } redirectId)
            {
                _logger.LogDebug("RedisNearCache found no subscriber connection on replica {EndPoint} yet; not pre-armed", endPoint);
                return;
            }

            var on = TrackingOnAsync(server, endPoint, redirectId, cancellationToken);
            var role = server.ExecuteAsync("ROLE");
            try
            {
                await on.ConfigureAwait(false);
            }
            catch
            {
                _ = role.ContinueWith(static t => _ = t.Exception, CancellationToken.None, TaskContinuationOptions.OnlyOnFaulted | TaskContinuationOptions.ExecuteSynchronously, TaskScheduler.Default);
                throw;
            }
            var roleResult = await role.WaitAsync(cancellationToken).ConfigureAwait(false);
            if (!IsReplicaRole(roleResult))
            {
                _logger.LogInformation("RedisNearCache found {EndPoint} is not a replica any more (ROLE {Role}); leaving it to the master arm", endPoint, Describe(roleResult));
                return;
            }

            if (!await VerifyAsync(server, endPoint, redirectId, cancellationToken).ConfigureAwait(false)) return;

            lock (_lifecycle)
            {
                if (Volatile.Read(ref _disposed) == 1) return;
                if (_connectionEpochs.GetValueOrDefault(endPoint) != epoch)
                {
                    // A connection to this node failed while the arm was in flight: the server has dropped the tracking
                    // (interactive) or the redirect target (subscriber). Not armed; the next sweep starts over.
                    _logger.LogDebug("RedisNearCache discarded the pre-arm of replica {EndPoint}: a connection failed meanwhile", endPoint);
                    return;
                }
                if (_redirectTargets.ContainsKey(endPoint) || _lost.ContainsKey(endPoint)) return;
                _replicaTargets[endPoint] = redirectId;
            }
            _logger.LogInformation("RedisNearCache pre-armed CLIENT TRACKING on replica {EndPoint} redirecting to client {RedirectClientId}", endPoint, redirectId);
        }
        finally
        {
            try { gate.Release(); } catch (ObjectDisposedException) { /* disposed underneath us */ }
        }
    }

    /// <summary>
    /// A <c>ROLE</c> reply on a replica is <c>[slave, master-ip, master-port, state, offset]</c>; the state is
    /// <c>connected</c> once the replication link is up and the initial sync is done. Valkey may spell the role
    /// <c>replica</c>.
    /// </summary>
    private static bool IsReplicaRole(RedisResult role, bool requireConnected = false)
    {
        try
        {
            if (role.IsNull || role.Resp2Type != ResultType.Array) return false;
            var items = (RedisResult[]?)role;
            if (items is not { Length: > 0 }) return false;
            var name = items[0].ToString();
            if (!string.Equals(name, "slave", StringComparison.OrdinalIgnoreCase) && !string.Equals(name, "replica", StringComparison.OrdinalIgnoreCase)) return false;
            if (!requireConnected) return true;
            return items.Length > 3 && string.Equals(items[3].ToString(), "connected", StringComparison.OrdinalIgnoreCase);
        }
        catch (InvalidCastException)
        {
            return false;
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
                if (error is not null && error is not ObjectDisposedException) MarkLostAndRetryLater(endPoint, reason);
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
    private static readonly TimeSpan SlowRetryInterval = TrackingRetry.SlowInterval;

    /// <summary>
    /// An arm exhausted its retries. Forget the (dead) redirect id so the endpoint is not mistaken for armed,
    /// raise <see cref="TrackingLost"/> so the facade stops caching, and keep retrying in the background.
    /// </summary>
    private void MarkLostAndRetryLater(EndPoint endPoint, ArmReason reason)
    {
        MarkLost(endPoint, forgetRedirect: true);
        RetryLater(endPoint, reason);
    }

    /// <summary>
    /// Starts (at most one per endpoint) a loop that re-tries the arm every <see cref="SlowRetryInterval"/>
    /// until the endpoint is armed, it stops being a master (then <see cref="EndpointRemoved"/> is raised), it
    /// was forgotten by another path, or we are disposed. A recovery is always reported as
    /// <see cref="ArmReason.Recovered"/> so the facade flushes: reads in flight while the node was untracked must
    /// be discarded.
    /// </summary>
    private void RetryLater(EndPoint endPoint, ArmReason originalReason)
    {
        var marker = new object();
        if (!_retrying.TryAdd(endPoint, marker)) return; // a loop is already running for this endpoint

        CancellationToken token;
        try { token = _shutdown.Token; } catch (ObjectDisposedException) { _retrying.TryRemove(KeyValuePair.Create(endPoint, marker)); return; }

        _ = Task.Run(async () =>
        {
            try
            {
                while (!token.IsCancellationRequested)
                {
                    await Task.Delay(SlowRetryInterval, token).ConfigureAwait(false);
                    if (!_lost.ContainsKey(endPoint)) return; // armed or forgotten meanwhile: the facade is not waiting on it
                    if (!await IsKnownMasterAsync(endPoint, token).ConfigureAwait(false))
                    {
                        _logger.LogInformation("RedisNearCache stopped retrying {EndPoint}: it is no longer a master of this deployment", endPoint);
                        RemoveEndpoint(endPoint);
                        return;
                    }
                    _logger.LogInformation("RedisNearCache retrying CLIENT TRACKING arm on {EndPoint} (originally {Reason})", endPoint, originalReason);
                    var error = await ArmWithRetryAsync(endPoint, ArmReason.Recovered, token).ConfigureAwait(false);
                    if (error is null || error is ObjectDisposedException) return;
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
            finally
            {
                _retrying.TryRemove(KeyValuePair.Create(endPoint, marker));
                // A failure that raced this loop's exit found the loop still registered and started none of its
                // own; without this the endpoint would stay lost with nobody resolving it.
                if (_lost.ContainsKey(endPoint) && !token.IsCancellationRequested && Volatile.Read(ref _disposed) == 0)
                    RetryLater(endPoint, originalReason);
            }
        }, CancellationToken.None);
    }

    /// <summary>
    /// Raises <see cref="TrackingLost"/> and records that the facade is now waiting on this endpoint. With
    /// <paramref name="forgetRedirect"/> the redirect id is dropped too (the server no longer honours it).
    /// </summary>
    private void MarkLost(EndPoint endPoint, bool forgetRedirect)
    {
        lock (_lifecycle)
        {
            if (forgetRedirect) _redirectTargets.TryRemove(endPoint, out _);
            _lost[endPoint] = 0;
            Raise(TrackingLost, endPoint, nameof(TrackingLost));
        }
    }

    /// <summary>
    /// Forgets an endpoint we armed or lost and raises <see cref="EndpointRemoved"/>, on which the facade flushes L1
    /// (entries read from that node are no longer protected by anything) and stops waiting on it. A no-op for an
    /// endpoint that is neither, so two paths deciding the same removal raise it once.
    /// </summary>
    private void RemoveEndpoint(EndPoint endPoint)
    {
        lock (_lifecycle)
        {
            var wasArmed = _redirectTargets.TryRemove(endPoint, out _);
            var wasLost = _lost.TryRemove(endPoint, out _);
            if (!wasArmed && !wasLost) return;
            _logger.LogInformation("RedisNearCache forgot endpoint {EndPoint} (was {State})", endPoint, wasArmed ? "armed" : "lost");
            Raise(EndpointRemoved, endPoint, nameof(EndpointRemoved));
        }
    }

    /// <summary>
    /// True when losing this endpoint's connections matters: we armed it or are waiting on it (whatever its role is
    /// now - a node flagged replica before its connections drop still holds tracking state for entries in L1), or
    /// it is a connected master.
    /// </summary>
    private bool IsTrackedMaster(EndPoint endPoint)
    {
        if (_redirectTargets.ContainsKey(endPoint) || _lost.ContainsKey(endPoint)) return true;
        try
        {
            var server = _connection.Multiplexer.GetServer(endPoint);
            if (server.IsReplica) return false;
            return MasterEndPoints().Contains(endPoint);
        }
        catch (Exception)
        {
            return false;
        }
    }

    /// <summary>
    /// True while the endpoint is still a master of the deployment (see <see cref="MasterRole"/>). For a disconnected
    /// cluster master this asks a connected node for <c>CLUSTER NODES</c>; when no node can answer it stays a master.
    /// </summary>
    private MasterProbe? _probe;

    private Task<bool> IsKnownMasterAsync(EndPoint endPoint, CancellationToken cancellationToken) =>
        (_probe ??= new MasterProbe(_connection.Multiplexer, _logger)).IsKnownMasterAsync(endPoint, cancellationToken);

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

    /// <summary>Snapshot of every server the multiplexer knows, for <see cref="MasterRole"/>. Empty on failure.</summary>
    private List<ServerView> ServerViews()
    {
        try
        {
            return MasterRole.ToViews(_connection.Multiplexer.GetServers());
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "RedisNearCache could not enumerate servers");
            return [];
        }
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
