using System.Collections.Concurrent;
using System.Diagnostics;
using System.Net;
using System.Net.Security;
using System.Net.Sockets;
using System.Security.Cryptography.X509Certificates;
using Microsoft.Extensions.Logging;
using RedisNearCache.Caching;
using RedisNearCache.Internal;
using StackExchange.Redis;

namespace RedisNearCache.Tracking.Broadcast;

/// <summary>
/// Tracking for Redis Enterprise-based services (Azure Managed Redis, Redis Cloud, Redis Software), whose proxy
/// rejects RESP2 tracking and <c>REDIRECT</c>. One RESP3 socket per master endpoint, armed with
/// <c>CLIENT TRACKING ON BCAST</c> and the configured key prefixes, is both the armer and the source of
/// invalidations: the server pushes <c>invalidate</c> frames onto the socket that asked for them, so this one
/// class implements <see cref="ITrackingArmer"/> and <see cref="IInvalidationListener"/>.
/// </summary>
/// <remarks>
/// <para>
/// <c>BCAST</c> decouples tracking from reading: the socket never issues a read, and the server broadcasts every
/// write under the configured prefixes to it. Invalidation volume therefore scales with the write rate under
/// those prefixes rather than with what L1 holds, which is why prefixes matter far more in this mode than in
/// <c>REDIRECT</c> mode. A <c>FLUSHDB</c>/<c>FLUSHALL</c> arrives as a push with a null key array.
/// </para>
/// <para>
/// The socket is outside the multiplexer (StackExchange.Redis 3.x consumes RESP3 <c>invalidate</c> pushes
/// internally) and outside the caller's connection entirely. Its TLS settings come from the same
/// <see cref="ConfigurationOptions"/> the private multiplexer uses: <see cref="ConfigurationOptions.Ssl"/>,
/// <see cref="ConfigurationOptions.SslHost"/>, <see cref="ConfigurationOptions.SslProtocols"/> and
/// <see cref="ConfigurationOptions.CheckCertificateRevocation"/>. <b>Custom certificate callbacks must be
/// supplied through <see cref="ConfigurationOptions.SslClientAuthenticationOptions"/></b>: the
/// <c>CertificateValidation</c> and <c>CertificateSelection</c> callbacks are events on
/// <see cref="ConfigurationOptions"/>, so nothing outside StackExchange.Redis can read them, and this socket
/// would otherwise fall back to the platform's default validation. When
/// <see cref="ConfigurationOptions.SslClientAuthenticationOptions"/> is set it is used verbatim, superseding
/// the individual settings above, exactly as it does on the library's own TLS path.
/// </para>
/// <para>
/// Tracking dies with the socket, so a socket that ends (read loop over, keepalive <c>PING</c> unanswered, write
/// failed) means the node is untracked until a new socket is armed: <see cref="TrackingLost"/> first, then a
/// reconnect, then <see cref="Armed"/> with <see cref="ArmReason.PushConnectionRestored"/>. Replicas are never
/// pre-armed - there is no redirect target to pre-point - so <see cref="ReplicaRedirectTargets"/> is always empty.
/// </para>
/// <para>
/// Credentials are read from the cloned <see cref="ConfigurationOptions"/> every time they are needed, never
/// captured once. That matters for services whose credential is short-lived: Microsoft.Azure.StackExchangeRedis
/// authenticates with a Microsoft Entra token by installing a <see cref="ConfigurationOptions.Defaults"/> provider
/// whose <c>User</c>/<c>Password</c> return the current token and refresh it before expiry, and
/// <c>ConfigurationOptions.Clone</c> carries that provider over by reference. So a reconnecting broadcast socket
/// authenticates with the token that is current at that moment, and a live one is re-authenticated in place -
/// a plain <c>AUTH</c>, which keeps the connection's <c>CLIENT TRACKING</c> state, not a second <c>HELLO</c> -
/// within one keepalive tick of a rotation (<see cref="KeepAliveInterval"/>, plus a <c>PING</c> round trip if one is
/// in flight), so the server never closes it at expiry and no
/// <see cref="TrackingLost"/>/flush cycle is paid per token lifetime. A credential the server rejects is kept out of
/// the way: the socket stays armed on the one it has, and the rotation is retried when the configuration yields another. The private multiplexer needs nothing from
/// this tracker: it is built from the same options, so the provider sees it through <c>AfterConnectAsync</c> and
/// re-authenticates it by its own mechanism (the Azure extension does; a hand-written provider must too).
/// </para>
/// </remarks>
internal sealed class BroadcastTracker : ITrackingArmer, IInvalidationListener
{
    /// <summary>Delays between arm attempts, shared with <c>Redirect</c> mode.</summary>
    private static readonly TimeSpan[] RetryDelays = TrackingRetry.Delays;

    /// <summary>How often each armed socket is pinged, to notice a connection that died without an EOF.</summary>
    internal static readonly TimeSpan KeepAliveInterval = TimeSpan.FromSeconds(10);

    /// <summary>How long a keepalive <c>PING</c> may take before the socket counts as dead.</summary>
    internal static readonly TimeSpan KeepAliveTimeout = TimeSpan.FromSeconds(5);

    /// <summary>How often the topology is reconciled with the multiplexer's view.</summary>
    internal static readonly TimeSpan ReconcileInterval = TimeSpan.FromSeconds(5);

    /// <summary>Interval between background retries once the fast backoff ladder has been exhausted.</summary>
    internal static readonly TimeSpan SlowRetryInterval = TrackingRetry.SlowInterval;

    private static readonly IReadOnlyDictionary<EndPoint, long> NoReplicas = new Dictionary<EndPoint, long>();

    private readonly RedisNearCacheConnection _connection;
    private readonly ILogger<BroadcastTracker> _logger;
    private readonly ConfigurationOptions _configuration;
    private readonly string _clientName;
    private readonly string[] _prefixes;
    private readonly string[] _droppedPrefixes;

    /// <summary>The armed socket per master endpoint. An entry means: tracking is on, on that socket.</summary>
    private readonly ConcurrentDictionary<EndPoint, Resp3Connection> _sockets = new();

    /// <summary>One gate per endpoint: two arms of the same node must never interleave.</summary>
    private readonly ConcurrentDictionary<EndPoint, SemaphoreSlim> _gates = new();

    /// <summary>
    /// Per endpoint, bumped whenever the current socket is dropped or the endpoint is forgotten. An arm publishes
    /// its socket only if the generation it started under is still current, so a slow attempt can never install a
    /// socket on top of a newer one.
    /// </summary>
    private readonly ConcurrentDictionary<EndPoint, long> _generations = new();

    /// <summary>Endpoints for which <see cref="TrackingLost"/> was the last lifecycle event raised.</summary>
    private readonly ConcurrentDictionary<EndPoint, byte> _lost = new();

    /// <summary>Endpoints that currently have a background slow-retry loop; at most one per endpoint.</summary>
    private readonly ConcurrentDictionary<EndPoint, object> _retrying = new();

    /// <summary>
    /// Endpoints with an arm in progress or queued, counted. The initial arm marks nothing lost and installs no
    /// socket until it succeeds, so without this the sweep would queue a second, loss-announcing arm on top of it.
    /// </summary>
    private readonly ConcurrentDictionary<EndPoint, int> _arming = new();

    /// <summary>Total in-place re-authentications across every socket this tracker has ever armed.</summary>
    private long _reauthentications;

    private readonly CancellationTokenSource _shutdown = new();
    private readonly TakeoverGuard _takeover;
    private readonly object _lifecycle = new();
    private readonly object _startLock = new();
    private Task? _start;
    private int _disposed;
    private bool _hooked;

    /// <summary>Creates the tracker. Nothing is connected until <see cref="StartAsync"/>.</summary>
    /// <param name="connection">The private multiplexer, used for topology and its events only.</param>
    /// <param name="options">The cache options; supplies the connection settings and the key prefixes.</param>
    /// <param name="logger">Logger.</param>
    public BroadcastTracker(RedisNearCacheConnection connection, RedisNearCacheOptions options, ILogger<BroadcastTracker> logger)
    {
        _connection = connection;
        _logger = logger;
        _configuration = RedisNearCacheConnection.BuildConfiguration(options);
        _clientName = $"{connection.ClientName}-bcast";
        _prefixes = NormalisePrefixes(options.EffectiveKeyPrefixes(), out _droppedPrefixes);
        _takeover = new TakeoverGuard(() => _connection.Multiplexer, _logger, MasterEndPoints, () => _sockets.Keys.ToArray(), Reconcile);
    }

    /// <inheritdoc />
    public event Action<TrackingArmedEvent>? Armed;

    /// <inheritdoc />
    public event Action<EndPoint>? TrackingLost;

    /// <inheritdoc />
    public event Action<EndPoint>? EndpointRemoved;

    /// <inheritdoc />
    public event Action<string>? KeyInvalidated;

    /// <inheritdoc />
    public event Action? FlushAll;

    /// <summary>The prefixes actually sent to <c>CLIENT TRACKING ... BCAST</c>; empty means the whole keyspace.</summary>
    internal IReadOnlyList<string> Prefixes => _prefixes;

    /// <summary>
    /// The span source of the cache this tracker belongs to, or null. Attached by the facade after construction
    /// (<see cref="AttachTracing"/>) rather than injected, because the tracker is built first. Null while a tracker is
    /// driven without a facade (tests, diagnostics): arming must not depend on it.
    /// </summary>
    private RedisNearCacheTracing? _tracing;

    /// <summary>Hands the tracker the tracing of the cache that owns it; see <see cref="_tracing"/>.</summary>
    public void AttachTracing(RedisNearCacheTracing tracing) => _tracing = tracing;

    /// <summary>
    /// How many times a live broadcast socket has been re-authenticated in place after the configuration handed
    /// out a rotated credential. Cumulative over every socket this tracker has armed, and never reset, so a socket
    /// that is replaced does not take its count with it.
    /// </summary>
    internal long Reauthentications => Interlocked.Read(ref _reauthentications);

    /// <inheritdoc />
    /// <remarks>
    /// The facade starts this instance twice, once as the listener and once as the armer. The first call owns the
    /// work; later calls return that same task (and so its result, or its failure), whatever token they pass.
    /// </remarks>
    public Task StartAsync(CancellationToken cancellationToken)
    {
        ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) == 1, this);
        lock (_startLock)
        {
            return _start ??= StartCoreAsync(cancellationToken);
        }
    }

    private async Task StartCoreAsync(CancellationToken cancellationToken)
    {
        if (_prefixes.Length == 0)
        {
            _logger.LogWarning(
                "RedisNearCache is arming CLIENT TRACKING in BCAST mode with no key prefix: the server will broadcast an invalidation for every write in the database to this client. Set {Option} to bound that traffic",
                $"{nameof(RedisNearCacheOptions)}.{nameof(RedisNearCacheOptions.KeyPrefixes)}");
        }

        if (_droppedPrefixes.Length > 0)
        {
            _logger.LogWarning(
                "RedisNearCache dropped the key prefix(es) {Dropped} from the BCAST arm: Redis rejects prefixes that overlap each other on one client, and the shorter prefix {Kept} already covers them",
                string.Join(", ", _droppedPrefixes), string.Join(", ", _prefixes));
        }

        HookEvents();
        try
        {
            await ArmAllAsync(ArmReason.Initial, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            // Even when nothing could be armed (no connected master yet): the sweep is what arms masters that
            // appear later, and a faulted start is memoised, so it must not depend on the start succeeding.
            StartReconcileLoop();
        }
    }

    /// <inheritdoc />
    public async Task RearmAsync(EndPoint endPoint, ArmReason reason, CancellationToken cancellationToken)
    {
        ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) == 1, this);

        using var linked = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, _shutdown.Token);
        var error = await ArmWithRetryAsync(endPoint, reason, announceLost: true, linked.Token).ConfigureAwait(false);
        if (error is null) return;

        // The endpoint was announced lost above and Reconcile leaves lost endpoints to their retry loop, so one
        // must exist or the node would stay in pass-through until its next socket death, which never comes.
        if (error is not ObjectDisposedException) MarkLostAndRetryLater(endPoint, reason);
        throw new RedisNearCacheTrackingException($"Could not arm CLIENT TRACKING BCAST on {endPoint} after {RetryDelays.Length + 1} attempts.", error);
    }

    /// <inheritdoc />
    public Task RearmAllAsync(ArmReason reason, CancellationToken cancellationToken)
    {
        ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) == 1, this);
        return ArmAllAsync(reason, cancellationToken);
    }

    /// <summary>
    /// Arms every connected master. Endpoints are armed concurrently (each has its own gate) and one failure never
    /// cancels the others. A partial failure is logged and retried in the background; failing everywhere (or
    /// finding no connected master at all) throws, because nothing would then be tracked.
    /// </summary>
    private async Task ArmAllAsync(ArmReason reason, CancellationToken cancellationToken)
    {
        using var linked = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, _shutdown.Token);
        var endPoints = MasterEndPoints();
        if (endPoints.Count == 0)
        {
            _logger.LogWarning("RedisNearCache found no connected master to arm CLIENT TRACKING BCAST on ({Reason})", reason);
            throw new RedisNearCacheTrackingException($"No connected master to arm CLIENT TRACKING BCAST on ({reason}).");
        }

        var results = await Task.WhenAll(endPoints.Select(ep => ArmWithRetryAsync(ep, reason, announceLost: true, linked.Token))).ConfigureAwait(false);
        var failures = new List<Exception>();
        for (var i = 0; i < endPoints.Count; i++)
        {
            if (results[i] is not { } error) continue;
            failures.Add(error);
            MarkLostAndRetryLater(endPoints[i], reason);
        }

        if (failures.Count == 0) return;

        if (failures.Count == endPoints.Count)
            throw new RedisNearCacheTrackingException($"Could not arm CLIENT TRACKING BCAST on any of the {endPoints.Count} connected master(s).", new AggregateException(failures));

        _logger.LogWarning(new AggregateException(failures),
            "RedisNearCache armed CLIENT TRACKING BCAST on {Armed} of {Total} master(s) ({Reason}); the cache stays in pass-through until the remaining node(s) are armed",
            endPoints.Count - failures.Count, endPoints.Count, reason);
    }

    /// <inheritdoc />
    public IReadOnlyDictionary<EndPoint, long> RedirectTargets
    {
        get
        {
            var snapshot = new Dictionary<EndPoint, long>();
            foreach (var pair in _sockets)
            {
                snapshot[pair.Key] = pair.Value.ClientId;
            }

            return snapshot;
        }
    }

    /// <inheritdoc />
    /// <remarks>Always empty: a broadcast socket tracks for itself, so there is nothing to pre-arm on a replica.</remarks>
    public IReadOnlyDictionary<EndPoint, long> ReplicaRedirectTargets => NoReplicas;

    /// <inheritdoc />
    /// <remarks>
    /// Always 0: broadcast mode never pre-arms a replica. There is no redirect target to point ahead of time - a
    /// broadcast socket tracks for itself - so there is no pre-arm that could fail, and a promotion is answered by
    /// the sweep arming the new master's own socket.
    /// </remarks>
    public long PreArmFailures => 0;

    // --- arming ---------------------------------------------------------------------------------------

    /// <summary>
    /// Arms one endpoint, retrying with backoff. Returns null on success, otherwise the last error. Never throws
    /// except for cancellation, so it is safe to call from an event handler or a background task.
    /// </summary>
    /// <param name="endPoint">The master to arm.</param>
    /// <param name="reason">Why, for the <see cref="Armed"/> event.</param>
    /// <param name="announceLost">
    /// False when the caller already raised <see cref="TrackingLost"/> for this endpoint (the socket-died path), so
    /// the facade sees exactly one loss per outage.
    /// </param>
    /// <param name="cancellationToken">Cancellation.</param>
    private async Task<Exception?> ArmWithRetryAsync(EndPoint endPoint, ArmReason reason, bool announceLost, CancellationToken cancellationToken)
    {
        // Any arm after the initial pass means tracking on this node is, or is about to be, unreliable. Say so
        // BEFORE waiting for the gate, so reads routed to this node meanwhile are not stored.
        if (announceLost && reason != ArmReason.Initial) MarkLost(endPoint);

        _arming.AddOrUpdate(endPoint, 1, static (_, n) => n + 1);
        var gate = _gates.GetOrAdd(endPoint, static _ => new SemaphoreSlim(1, 1));
        try
        {
            await gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (ObjectDisposedException ex)
        {
            DoneArming(endPoint);
            return ex; // disposed while we were queued: not a success
        }

        try
        {
            Exception? lastError = null;
            for (var attempt = 0; attempt <= RetryDelays.Length; attempt++)
            {
                cancellationToken.ThrowIfCancellationRequested();
                if (Volatile.Read(ref _disposed) == 1) return new ObjectDisposedException(nameof(BroadcastTracker));
                if (attempt > 0)
                {
                    var delay = RetryDelays[attempt - 1];
                    _logger.LogWarning(lastError,
                        "RedisNearCache retrying the CLIENT TRACKING BCAST arm on {EndPoint} ({Reason}) in {Delay} ms, attempt {Attempt} of {Attempts}",
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
                    // The server rejected the command itself (bad prefix, no permission, no RESP3): retrying now
                    // cannot help, so leave it to the slow loop rather than hammering the node.
                    _logger.LogWarning(ex, "RedisNearCache cannot arm CLIENT TRACKING BCAST on {EndPoint} ({Reason}); giving up the fast retries", endPoint, reason);
                    return ex;
                }
                catch (Exception ex)
                {
                    lastError = ex;
                }
            }

            _logger.LogWarning(lastError,
                "RedisNearCache gave up arming CLIENT TRACKING BCAST on {EndPoint} ({Reason}) after {Attempts} attempts; invalidations from this node will not be received until it is re-armed",
                endPoint, reason, RetryDelays.Length + 1);
            return lastError ?? new RedisNearCacheTrackingException($"Could not arm CLIENT TRACKING BCAST on {endPoint}.");
        }
        finally
        {
            try { gate.Release(); } catch (ObjectDisposedException) { /* disposed underneath us */ }
            DoneArming(endPoint);
        }
    }

    private void DoneArming(EndPoint endPoint)
    {
        lock (_lifecycle)
        {
            if (_arming.TryGetValue(endPoint, out var n))
            {
                if (n <= 1) _arming.TryRemove(endPoint, out _);
                else _arming[endPoint] = n - 1;
            }
        }
    }

    /// <summary>
    /// One attempt: open a RESP3 socket, <c>HELLO 3</c>, read its <c>CLIENT ID</c>, arm
    /// <c>CLIENT TRACKING ON BCAST</c> with the configured prefixes, and verify with <c>CLIENT TRACKINGINFO</c>
    /// that the server really has tracking on in broadcast mode before anything is published or announced.
    /// </summary>
    private async Task<bool> TryArmAsync(EndPoint endPoint, ArmReason reason, CancellationToken cancellationToken)
    {
        // Taken before the socket exists: anything that drops this endpoint while we connect bumps it, and the
        // publish below then throws the new socket away instead of installing it over a newer one.
        var generation = NextGeneration(endPoint);

        var socket = await ConnectAsync(endPoint, cancellationToken).ConfigureAwait(false);
        var published = false;
        // The handshake replies get a deadline of their own: a peer that completes the TCP handshake and then goes
        // quiet must not hold the arm (and the facade's Ready) open forever. The keepalive only starts once armed.
        var handshakeTimeout = _configuration.SyncTimeout > 0 ? _configuration.SyncTimeout : 5_000;
        using var handshake = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        handshake.CancelAfter(handshakeTimeout);
        var token = handshake.Token;
        // The same span, name and attributes as the REDIRECT armer's, so a trace query need not know the mode. It
        // covers exactly the arm on this socket - the handshake, CLIENT TRACKING ON BCAST and the TRACKINGINFO that
        // verifies it - and ends BEFORE Publish below, which raises Armed under the lifecycle lock and runs the
        // facade's flush handler: the invariant is that no flush handler ever runs with this span current. One span per
        // ATTEMPT, as in REDIRECT mode, so the retry ladder above shows up as several. Nothing on a timer is traced:
        // no span on the reconcile sweep, none on the keepalive PING. rnc.redirect_client_id is omitted - a BCAST
        // socket tracks for itself, so there is no redirect target to report.
        var activity = _tracing?.StartArm(endPoint, reason);
        try
        {
            // Read once, here, and remembered on the socket: the configuration can hand out a different credential
            // on every read (a rotating Defaults provider), and the watchdog compares against what was actually
            // sent, not against whatever the provider happens to return later.
            var credentials = new BroadcastCredentials(_configuration.User, _configuration.Password);
            socket.Authenticated = credentials;

            var hello = new List<string> { "HELLO", "3" };
            if (credentials.HasPassword)
            {
                hello.Add("AUTH");
                hello.Add(credentials.User);
                hello.Add(credentials.Password!);
            }

            hello.Add("SETNAME");
            hello.Add(_clientName);

            var greeting = await socket.ExecuteAsync(token, hello.ToArray()).ConfigureAwait(false);
            var proto = Field(greeting, "proto");
            if (proto is null || proto.Integer != 3)
                throw new RedisNearCacheTrackingException($"HELLO 3 on {endPoint} did not negotiate RESP3 (proto={proto?.ToString() ?? "absent"}).");

            var id = await socket.ExecuteAsync(token, "CLIENT", "ID").ConfigureAwait(false);
            if (id.Kind != Resp3Kind.Integer)
                throw new RedisNearCacheTrackingException($"CLIENT ID on {endPoint} returned {id.Kind}, expected an integer.");
            socket.ClientId = id.Integer;

            // A fresh socket never has tracking on, so no CLIENT TRACKING OFF is needed first (which is what a
            // re-arm on the same socket would need: the server rejects an overlapping prefix while tracking is on).
            var arm = new List<string> { "CLIENT", "TRACKING", "ON", "BCAST" };
            foreach (var prefix in _prefixes)
            {
                arm.Add("PREFIX");
                arm.Add(prefix);
            }

            await socket.ExecuteAsync(token, arm.ToArray()).ConfigureAwait(false);

            Resp3Value? info = null;
            try
            {
                info = await socket.ExecuteAsync(token, "CLIENT", "TRACKINGINFO").ConfigureAwait(false);
            }
            catch (RedisNearCacheTrackingException ex)
            {
                // The server answered the arm with OK but rejected TRACKINGINFO (Redis 6.0/6.1, or a proxy that
                // restricts the subcommand): assume the arm took, as the redirect armer does. Only a server error
                // reply lands here; a dead socket surfaces as a different exception and is retried.
                _logger.LogDebug(ex, "RedisNearCache could not read CLIENT TRACKINGINFO on {EndPoint}; assuming the BCAST arm took", endPoint);
            }

            var flags = info is null ? ["on", "bcast", "(unverified)"] : ReadFlags(info);
            if (!flags.Contains("on", StringComparer.OrdinalIgnoreCase) || !flags.Contains("bcast", StringComparer.OrdinalIgnoreCase))
            {
                // The same span status the REDIRECT armer records for an arm the server would not confirm.
                RedisNearCacheTracing.RecordArmNotVerified(activity);
                throw new RedisNearCacheTrackingException($"CLIENT TRACKINGINFO on {endPoint} reported flags [{string.Join(",", flags)}], expected 'on' and 'bcast'.");
            }

            // The arm is done and verified: close the span here, so Publish (which raises Armed, and with it the
            // facade's flush) never runs with it current. The finally below closes it on every failure path instead.
            activity?.Dispose();
            activity = null;

            published = Publish(endPoint, socket, generation, reason);
            if (!published)
            {
                _logger.LogDebug("RedisNearCache discarded a broadcast socket for {EndPoint}: the endpoint moved on while it was being armed", endPoint);
                return false;
            }

            _logger.LogInformation(
                "RedisNearCache armed CLIENT TRACKING ON BCAST on {EndPoint} ({Reason}) as client {ClientId} ({ClientName}), prefixes [{Prefixes}], flags [{Flags}]",
                endPoint, reason, socket.ClientId, _clientName,
                _prefixes.Length == 0 ? "<whole keyspace>" : string.Join(",", _prefixes), string.Join(",", flags));
            WatchSocket(endPoint, socket);
            return true;
        }
        catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
        {
            var timedOut = new TimeoutException($"The broadcast socket to {endPoint} did not complete the tracking handshake within {handshakeTimeout} ms.");
            RedisNearCacheTracing.RecordFailure(activity, timedOut);
            throw timedOut;
        }
        catch (Exception ex)
        {
            // Recorded and rethrown untouched: the retry ladder in ArmWithRetryAsync is what decides what happens
            // next. Left alone when the verification above already recorded a more specific status.
            if (activity?.Status != ActivityStatusCode.Error) RedisNearCacheTracing.RecordFailure(activity, ex);
            throw;
        }
        finally
        {
            // Null already if the arm was verified and the span closed before Publish; still open on every other path.
            activity?.Dispose();
            if (!published) await DisposeSocketAsync(socket).ConfigureAwait(false);
        }
    }

    /// <summary>Installs a freshly armed socket, unless the endpoint moved on meanwhile. Raises <see cref="Armed"/>.</summary>
    private bool Publish(EndPoint endPoint, Resp3Connection socket, long generation, ArmReason reason)
    {
        Resp3Connection? replaced = null;
        lock (_lifecycle)
        {
            if (Volatile.Read(ref _disposed) == 1) return false;
            if (_generations.TryGetValue(endPoint, out var current) && current != generation) return false;
            if (_sockets.TryGetValue(endPoint, out var existing) && !ReferenceEquals(existing, socket)) replaced = existing;

            _sockets[endPoint] = socket;
            _lost.TryRemove(endPoint, out _);
            Raise(Armed, new TrackingArmedEvent(endPoint, socket.ClientId, reason), nameof(Armed));
        }

        if (replaced is not null) FireAndForgetDispose(replaced);
        return true;
    }

    /// <summary>Opens a TCP (and, when configured, TLS) connection to one endpoint and starts its read loop.</summary>
    private async Task<Resp3Connection> ConnectAsync(EndPoint endPoint, CancellationToken cancellationToken)
    {
        var timeout = _configuration.ConnectTimeout > 0 ? _configuration.ConnectTimeout : 5_000;
        using var linked = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        linked.CancelAfter(timeout);
        var token = linked.Token;

        Socket socket = endPoint switch
        {
            IPEndPoint ip => new Socket(ip.AddressFamily, SocketType.Stream, ProtocolType.Tcp),
            DnsEndPoint => new Socket(SocketType.Stream, ProtocolType.Tcp),
            _ => throw new RedisNearCacheTrackingException($"RedisNearCache cannot open a broadcast socket to {endPoint} ({endPoint.GetType().Name} is not a TCP endpoint)."),
        };

        Stream? stream = null;
        try
        {
            socket.NoDelay = true;
            switch (endPoint)
            {
                case IPEndPoint ip:
                    await socket.ConnectAsync(ip, token).ConfigureAwait(false);
                    break;
                case DnsEndPoint dns:
                    await socket.ConnectAsync(dns.Host, dns.Port, token).ConfigureAwait(false);
                    break;
            }

            stream = new NetworkStream(socket, ownsSocket: false);
            if (_configuration.Ssl)
            {
                var host = ResolveTlsHost(endPoint);
                var ssl = new SslStream(stream, leaveInnerStreamOpen: false);
                stream = ssl;
                await ssl.AuthenticateAsClientAsync(BuildSslOptions(host), token).ConfigureAwait(false);
            }

            var connection = new Resp3Connection(stream, endPoint.ToString() ?? "endpoint", value => OnPush(endPoint, value), _logger, socket);
            connection.Start();
            return connection;
        }
        catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
        {
            if (stream is not null) await stream.DisposeAsync().ConfigureAwait(false);
            socket.Dispose();
            throw new TimeoutException($"Connecting a broadcast socket to {endPoint} timed out after {timeout} ms.");
        }
        catch
        {
            if (stream is not null) await stream.DisposeAsync().ConfigureAwait(false);
            socket.Dispose();
            throw;
        }
    }

    /// <summary>
    /// The TLS options for the broadcast socket: the caller's own
    /// <see cref="ConfigurationOptions.SslClientAuthenticationOptions"/> when it supplies them (it supersedes
    /// everything else, as on the library's own TLS path), otherwise the host, protocols and revocation setting
    /// the configuration carries, with the platform's default certificate validation. The configuration's
    /// certificate callbacks are events and cannot be read from here; see the remarks on the class.
    /// </summary>
    private SslClientAuthenticationOptions BuildSslOptions(string host)
    {
        if (_configuration.SslClientAuthenticationOptions?.Invoke(host) is { } supplied) return supplied;

        var options = new SslClientAuthenticationOptions
        {
            TargetHost = host,
            CertificateRevocationCheckMode = _configuration.CheckCertificateRevocation ? X509RevocationMode.Online : X509RevocationMode.NoCheck,
        };
        if (_configuration.SslProtocols is { } protocols) options.EnabledSslProtocols = protocols;
        return options;
    }

    /// <summary>
    /// The TLS host name for an endpoint: the configured <see cref="ConfigurationOptions.SslHost"/> if there is
    /// one, otherwise the host part of the endpoint - what the library's own TLS path does.
    /// </summary>
    private string ResolveTlsHost(EndPoint endPoint)
    {
        if (_configuration.SslHost is { Length: > 0 } configured) return configured;
        return endPoint switch
        {
            DnsEndPoint dns => dns.Host,
            IPEndPoint ip => ip.Address.ToString(),
            _ => endPoint.ToString() ?? string.Empty,
        };
    }

    // --- failure detection ----------------------------------------------------------------------------

    /// <summary>
    /// Watches one armed socket: it is dead when its read loop ends, when a keepalive <c>PING</c> is not answered
    /// within <see cref="KeepAliveTimeout"/>, or when the write fails. Tracking died with it, so the endpoint is
    /// announced lost and re-armed on a new socket.
    /// </summary>
    /// <remarks>
    /// The same tick also carries credential rotation: the configuration's current user and password are compared
    /// against what this socket authenticated with at the start of each iteration, before the <c>PING</c>, and no
    /// timer of its own is involved - so rotated credentials reach a live socket within
    /// <see cref="KeepAliveInterval"/>. A rotation the server rejects keeps the socket armed on its current credential; only the <c>PING</c> decides liveness.
    /// </remarks>
    private void WatchSocket(EndPoint endPoint, Resp3Connection socket)
    {
        if (!TryGetShutdownToken(out var token)) return;

        _ = Task.Run(async () =>
        {
            string cause;
            try
            {
                while (true)
                {
                    var finished = await Task.WhenAny(socket.Completion, Task.Delay(KeepAliveInterval, token)).ConfigureAwait(false);
                    if (token.IsCancellationRequested) return;
                    if (ReferenceEquals(finished, socket.Completion))
                    {
                        cause = "its read loop ended";
                        break;
                    }

                    // Before the PING, so a socket whose credential expired mid-interval is re-authenticated on the
                    // same tick that would otherwise find it closed by the server. Never fatal by itself: the PING
                    // that follows is what decides whether the socket is alive.
                    await ReauthenticateIfRotatedAsync(endPoint, socket, token).ConfigureAwait(false);

                    var (outcome, error) = await ExecuteBoundedAsync(socket, token, "PING").ConfigureAwait(false);
                    if (outcome == BoundedOutcome.Ok) continue;
                    if (error is not null) _logger.LogDebug(error, "RedisNearCache keepalive PING failed on the broadcast socket to {EndPoint}", endPoint);
                    cause = outcome == BoundedOutcome.TimedOut
                        ? $"it did not answer PING within {KeepAliveTimeout.TotalSeconds:0.#} s"
                        : "its keepalive PING failed";
                    break;
                }
            }
            catch (OperationCanceledException)
            {
                return; // shutting down
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "RedisNearCache stopped watching the broadcast socket to {EndPoint} unexpectedly", endPoint);
                cause = "its watchdog failed";
            }

            try
            {
                OnSocketDied(endPoint, socket, cause);
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "RedisNearCache failed to handle the loss of the broadcast socket to {EndPoint}", endPoint);
            }
        }, CancellationToken.None);
    }

    /// <summary>
    /// Re-authenticates one live socket in place when the configuration now yields a credential different from the
    /// one it authenticated with. A no-op, and no round trip, in the common case where nothing rotated.
    /// </summary>
    /// <remarks>
    /// A plain <c>AUTH</c>, never a second <c>HELLO</c>: re-authenticating an already authenticated Redis connection
    /// leaves its <c>CLIENT TRACKING</c> state alone (<c>CLIENT TRACKINGINFO</c> still reports <c>on</c>, <c>bcast</c>
    /// and the same prefixes afterwards), so the socket stays armed across a rotation and no invalidation is missed.
    /// Nor is a failure fatal: a rejected <c>AUTH</c> leaves the connection authenticated as before, so the socket keeps
    /// working and stays armed, the rejected pair is remembered so it is not retried every tick, and the rotation is
    /// tried again as soon as the configuration yields something else. A closed socket is left to the keepalive
    /// <c>PING</c> to diagnose, and the server closing the connection at credential expiry is an ordinary socket death.
    /// The configuration's <c>User</c> and <c>Password</c> are two reads, not one atomic pair: a provider rotating both
    /// could in principle be observed mid-update, which costs one rejected attempt and a retry on the next tick.
    /// </remarks>
    private async Task ReauthenticateIfRotatedAsync(EndPoint endPoint, Resp3Connection socket, CancellationToken token)
    {
        var current = new BroadcastCredentials(_configuration.User, _configuration.Password);
        if (current == socket.Authenticated || current == socket.Rejected) return;

        if (!current.HasPassword)
        {
            // A live connection cannot be de-authenticated, and a rotation to "no password" is not something a
            // token provider does; keep the credential the socket has, and say so once.
            if (socket.Authenticated?.HasPassword == true)
                _logger.LogWarning("RedisNearCache: the configuration now yields no password for the broadcast socket to {EndPoint}; keeping the credential it authenticated with", endPoint);
            socket.Rejected = current;
            return;
        }

        var (outcome, error) = await ExecuteBoundedAsync(socket, token, "AUTH", current.User, current.Password!).ConfigureAwait(false);
        switch (outcome)
        {
            case BoundedOutcome.Ok:
                socket.Authenticated = current;
                socket.Rejected = null;
                Interlocked.Increment(ref _reauthentications);
                _logger.LogInformation("RedisNearCache re-authenticated the broadcast socket to {EndPoint} with rotated credentials for user {User}", endPoint, current.User);
                break;
            case BoundedOutcome.Rejected:
                socket.Rejected = current;
                _logger.LogWarning(error, "RedisNearCache: the server rejected the rotated credentials for user {User} on the broadcast socket to {EndPoint}; keeping the credential it authenticated with until the configuration yields another", current.User, endPoint);
                break;
            case BoundedOutcome.TimedOut:
                // Not remembered as rejected: a slow reply (a push backlog on the read loop, say) is worth another try.
                _logger.LogWarning("RedisNearCache did not get an answer to AUTH on the broadcast socket to {EndPoint} within {Timeout} s while applying rotated credentials for user {User}; will retry", endPoint, KeepAliveTimeout.TotalSeconds, current.User);
                break;
            default:
                _logger.LogDebug(error, "RedisNearCache could not send AUTH on the broadcast socket to {EndPoint}: it is closed; the keepalive decides its fate", endPoint);
                break;
        }
    }

    private enum BoundedOutcome
    {
        /// <summary>The command was answered without error.</summary>
        Ok,
        /// <summary>The server answered with an error reply.</summary>
        Rejected,
        /// <summary>No reply within <see cref="KeepAliveTimeout"/>.</summary>
        TimedOut,
        /// <summary>The socket is closed or the write failed.</summary>
        Closed,
    }

    /// <summary>
    /// Sends one command on an armed socket with the keepalive deadline and classifies the result instead of throwing,
    /// so the <c>PING</c> and <c>AUTH</c> paths share one timeout and one reading of the failure. Only shutdown escapes.
    /// </summary>
    private async Task<(BoundedOutcome Outcome, Exception? Error)> ExecuteBoundedAsync(Resp3Connection socket, CancellationToken token, params string[] args)
    {
        using var deadline = CancellationTokenSource.CreateLinkedTokenSource(token);
        deadline.CancelAfter(KeepAliveTimeout);
        try
        {
            await socket.ExecuteAsync(deadline.Token, args).ConfigureAwait(false);
            return (BoundedOutcome.Ok, null);
        }
        catch (OperationCanceledException) when (token.IsCancellationRequested)
        {
            throw; // shutting down; the watchdog's own handler returns
        }
        catch (OperationCanceledException ex)
        {
            return (BoundedOutcome.TimedOut, ex);
        }
        catch (RedisNearCacheTrackingException ex)
        {
            return (BoundedOutcome.Rejected, ex); // the server's error reply, with its message
        }
        catch (Exception ex)
        {
            return (BoundedOutcome.Closed, ex); // ObjectDisposedException, the connection's own closed-socket error, IO
        }
    }

    /// <summary>
    /// One socket died: forget it, announce the loss once, and reconnect. A socket that is no longer the one
    /// installed for this endpoint (already replaced, or the endpoint was removed) is only disposed.
    /// </summary>
    private void OnSocketDied(EndPoint endPoint, Resp3Connection socket, string cause)
    {
        if (Volatile.Read(ref _disposed) == 1) return;

        bool wasCurrent;
        lock (_lifecycle)
        {
            wasCurrent = _sockets.TryGetValue(endPoint, out var installed) && ReferenceEquals(installed, socket);
            if (wasCurrent)
            {
                _sockets.TryRemove(endPoint, out _);
                NextGeneration(endPoint);
                if (_lost.TryAdd(endPoint, 0)) Raise(TrackingLost, endPoint, nameof(TrackingLost));
            }
        }

        FireAndForgetDispose(socket);
        if (!wasCurrent) return;

        _logger.LogWarning("RedisNearCache lost the broadcast socket to {EndPoint} ({Cause}); tracking there is off until it is re-armed", endPoint, cause);
        QueueArm(endPoint, ArmReason.PushConnectionRestored, announceLost: false);
    }

    // --- topology -------------------------------------------------------------------------------------

    private void HookEvents()
    {
        if (_hooked) return;
        _connection.Multiplexer.ConfigurationChanged += OnConfigurationChanged;
        _connection.Multiplexer.ConfigurationChangedBroadcast += OnConfigurationChanged;
        _connection.Multiplexer.ConnectionRestored += OnConnectionRestored;
        _connection.Multiplexer.ConnectionFailed += OnConnectionFailed;
        _hooked = true;
    }

    private void UnhookEvents()
    {
        if (!_hooked) return;
        try
        {
            _connection.Multiplexer.ConfigurationChanged -= OnConfigurationChanged;
            _connection.Multiplexer.ConfigurationChangedBroadcast -= OnConfigurationChanged;
            _connection.Multiplexer.ConnectionRestored -= OnConnectionRestored;
            _connection.Multiplexer.ConnectionFailed -= OnConnectionFailed;
        }
        catch (Exception ex)
        {
            _logger.LogDebug(ex, "RedisNearCache could not unhook the multiplexer events");
        }

        _hooked = false;
    }

    private void OnConfigurationChanged(object? sender, EndPointEventArgs e) => Reconcile("configuration change");

    private void OnConnectionRestored(object? sender, ConnectionFailedEventArgs e) => Reconcile("connection restored");

    /// <summary>
    /// The private multiplexer lost a connection to a master. The broadcast socket to that node is a separate
    /// connection and may still be alive, but the repo rule is that any lost connection to a master means re-arm
    /// then flush: treating the socket as lost costs one flush if the node is fine, and closes a window of up to
    /// the keepalive interval plus its timeout during which a partitioned node would otherwise be served from L1.
    /// </summary>
    private void OnConnectionFailed(object? sender, ConnectionFailedEventArgs e)
    {
        try
        {
            if (Volatile.Read(ref _disposed) == 1 || e.EndPoint is not { } endPoint) return;
            if (!_sockets.ContainsKey(endPoint)) return;
            _logger.LogWarning("RedisNearCache saw the private multiplexer lose its {ConnectionType} connection to {EndPoint}; treating the broadcast socket there as lost", e.ConnectionType, endPoint);
            MarkLost(endPoint);
            QueueArm(endPoint, ArmReason.PushConnectionRestored, announceLost: false);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "RedisNearCache failed to handle a connection failure on {EndPoint}", e.EndPoint);
        }
    }

    /// <summary>
    /// Brings the tracker in line with the multiplexer's current view: arm any connected master that has no
    /// socket, and retire every endpoint that is no longer a master of this deployment, once whoever serves now is
    /// armed (<see cref="RetireWhenTakeoverArmed"/>). Idempotent and cheap when nothing changed. Endpoints already
    /// announced lost are left to their retry loop rather than queued again.
    /// </summary>
    private void Reconcile(string cause)
    {
        try
        {
            if (Volatile.Read(ref _disposed) == 1) return;

            var masters = MasterEndPoints();
            // Queued only once every new master of this pass has been announced lost: the first one's Armed may be what
            // takes the facade out of a failed start, and it must find the others in the lost set by then.
            var toArm = new List<EndPoint>();
            foreach (var endPoint in masters)
            {
                if (_sockets.ContainsKey(endPoint) || _lost.ContainsKey(endPoint) || _arming.ContainsKey(endPoint)) continue;
                _logger.LogInformation("RedisNearCache saw a {Cause} adding master {EndPoint}; arming CLIENT TRACKING BCAST", cause, endPoint);
                // Announced here rather than on the arm's own task: reads may already be routed to the new master, and
                // the facade must not store them before its socket is armed.
                MarkLost(endPoint);
                toArm.Add(endPoint);
            }

            foreach (var endPoint in toArm) QueueArm(endPoint, ArmReason.TopologyChanged, announceLost: false);

            var candidates = _sockets.Keys.Concat(_lost.Keys).Distinct().Where(ep => !masters.Contains(ep)).ToArray();
            if (candidates.Length == 0) return;

            var servers = ServerViews();
            // No view of the deployment: never forget endpoints on the strength of nothing.
            if (servers.Count == 0) return;
            foreach (var endPoint in candidates)
            {
                // null means "disconnected cluster master, cannot tell from here": leave it to the retry loop.
                if (MasterRole.FromMultiplexer(endPoint, servers) is not false) continue;
                RetireWhenTakeoverArmed(endPoint, cause);
            }
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "RedisNearCache failed to handle a {Cause}", cause);
        }
    }

    /// <summary>
    /// Starts (at most one per endpoint) a loop that forgets an armed or lost endpoint the multiplexer reports as no
    /// longer a master, but only once <see cref="TakeoverGuard.TakeoverArmedAsync"/> finds every master serving now armed: the
    /// multiplexer can learn a demotion (a manual failover, or a killed master that restarted as a replica) in the
    /// same reconfigure that reveals the promoted node, and forgetting the old one first would let the facade serve
    /// from L1 while writes to the new one produce no invalidations. A demoted node that is still armed keeps its
    /// socket meanwhile: as a replica it still pushes invalidations for the writes it replicates. The loop stops
    /// without forgetting anything if the endpoint is forgotten elsewhere or the multiplexer no longer reports it
    /// as a non-master.
    /// </summary>
    private void RetireWhenTakeoverArmed(EndPoint endPoint, string cause)
    {
        if (!TryGetShutdownToken(out var token)) return;
        _takeover.RetireWhenArmed(endPoint, cause, StillRetiring, () => RemoveEndpoint(endPoint), Abandoned, token);

        bool StillRetiring()
        {
            if (Volatile.Read(ref _disposed) == 1) return false;
            if (!_sockets.ContainsKey(endPoint) && !_lost.ContainsKey(endPoint)) return false; // forgotten elsewhere
            var servers = ServerViews();
            // No view, or no longer a non-master (a master again, or a disconnected cluster node): the reconcile or the
            // retry loop decides from here.
            return servers.Count > 0 && MasterRole.FromMultiplexer(endPoint, servers) is false;
        }

        // An endpoint still announced lost needs someone to resolve it; the retry loop is a no-op if it is running.
        void Abandoned()
        {
            if (_lost.ContainsKey(endPoint) && Volatile.Read(ref _disposed) == 0) RetryLater(endPoint, ArmReason.TopologyChanged);
        }
    }

    /// <summary>Every <see cref="ReconcileInterval"/>: <see cref="Reconcile"/>.</summary>
    private void StartReconcileLoop()
    {
        if (!TryGetShutdownToken(out var token)) return;

        _ = Task.Run(async () =>
        {
            try
            {
                while (!token.IsCancellationRequested)
                {
                    await Task.Delay(ReconcileInterval, token).ConfigureAwait(false);
                    Reconcile("topology check");
                }
            }
            catch (OperationCanceledException)
            {
                // shutting down
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "RedisNearCache broadcast reconcile loop stopped unexpectedly");
            }
        }, CancellationToken.None);
    }

    /// <summary>Arms one endpoint on a background task, and keeps retrying in the background if that fails.</summary>
    private void QueueArm(EndPoint endPoint, ArmReason reason, bool announceLost)
    {
        if (!TryGetShutdownToken(out var token)) return;

        _ = Task.Run(async () =>
        {
            try
            {
                var error = await ArmWithRetryAsync(endPoint, reason, announceLost, token).ConfigureAwait(false);
                if (error is not null && error is not ObjectDisposedException) MarkLostAndRetryLater(endPoint, reason);
            }
            catch (OperationCanceledException)
            {
                // shutting down
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "RedisNearCache failed to arm CLIENT TRACKING BCAST on {EndPoint} ({Reason})", endPoint, reason);
            }
        }, CancellationToken.None);
    }

    /// <summary>An arm exhausted its fast retries: stay in pass-through for this node and keep trying slowly.</summary>
    private void MarkLostAndRetryLater(EndPoint endPoint, ArmReason reason)
    {
        MarkLost(endPoint);
        RetryLater(endPoint, reason);
    }

    /// <summary>
    /// Starts (at most one per endpoint) a loop that re-tries the arm every <see cref="SlowRetryInterval"/> until
    /// the endpoint is armed, stops being a master (then <see cref="EndpointRemoved"/> is raised), is forgotten by
    /// another path, or we are disposed. A recovery is reported as <see cref="ArmReason.Recovered"/>, so the facade
    /// flushes what was read while the node was untracked.
    /// </summary>
    private void RetryLater(EndPoint endPoint, ArmReason originalReason)
    {
        var marker = new object();
        if (!_retrying.TryAdd(endPoint, marker)) return; // a loop is already running for this endpoint

        if (!TryGetShutdownToken(out var token)) { _retrying.TryRemove(KeyValuePair.Create(endPoint, marker)); return; }

        _ = Task.Run(async () =>
        {
            try
            {
                var takeoverRounds = 0;
                while (!token.IsCancellationRequested)
                {
                    await Task.Delay(SlowRetryInterval, token).ConfigureAwait(false);
                    if (!_lost.ContainsKey(endPoint)) return; // armed or forgotten meanwhile
                    // A disconnected cluster master looks like a master to the multiplexer forever; the shared probe
                    // asks a connected node for CLUSTER NODES, and keeps the endpoint when nothing can answer.
                    var probe = await _takeover.Probe.ProbeAsync(endPoint, token).ConfigureAwait(false);
                    if (!probe.IsKnownMaster)
                    {
                        // The reconcile is already retiring it under the same guard: one waiter is enough.
                        if (_takeover.IsRetiring(endPoint)) continue;
                        if (!await _takeover.TakeoverArmedAsync(endPoint, probe.ClusterNodes, ++takeoverRounds, token).ConfigureAwait(false)) continue;
                        _logger.LogInformation("RedisNearCache stopped retrying {EndPoint}: it is no longer a master of this deployment", endPoint);
                        RemoveEndpoint(endPoint);
                        return;
                    }

                    _logger.LogInformation("RedisNearCache retrying the CLIENT TRACKING BCAST arm on {EndPoint} (originally {Reason})", endPoint, originalReason);
                    var error = await ArmWithRetryAsync(endPoint, ArmReason.Recovered, announceLost: false, token).ConfigureAwait(false);
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
                // A failure that raced this loop's exit found it still registered and started none of its own.
                if (_lost.ContainsKey(endPoint) && !token.IsCancellationRequested && Volatile.Read(ref _disposed) == 0)
                    RetryLater(endPoint, originalReason);
            }
        }, CancellationToken.None);
    }

    /// <summary>
    /// Records that the facade is now waiting on this endpoint, raising <see cref="TrackingLost"/> if it was not
    /// already; a re-arm of a node whose socket already died announces nothing new. The socket,
    /// if any, is dropped with it: on this transport tracking and socket live and die together.
    /// </summary>
    private void MarkLost(EndPoint endPoint)
    {
        Resp3Connection? dropped = null;
        lock (_lifecycle)
        {
            if (_sockets.TryRemove(endPoint, out var socket)) dropped = socket;
            NextGeneration(endPoint);
            // Raised once per outage: a re-arm that fails after the socket already died must not announce twice.
            if (_lost.TryAdd(endPoint, 0)) Raise(TrackingLost, endPoint, nameof(TrackingLost));
        }

        if (dropped is not null) FireAndForgetDispose(dropped);
    }

    /// <summary>
    /// Forgets an endpoint we armed or lost and raises <see cref="EndpointRemoved"/>, on which the facade flushes
    /// L1 and stops waiting on it. A no-op for an endpoint that is neither.
    /// </summary>
    private void RemoveEndpoint(EndPoint endPoint)
    {
        Resp3Connection? dropped = null;
        bool removed;
        lock (_lifecycle)
        {
            var wasArmed = _sockets.TryRemove(endPoint, out var socket);
            if (wasArmed) dropped = socket;
            var wasLost = _lost.TryRemove(endPoint, out _);
            removed = wasArmed || wasLost;
            if (removed)
            {
                NextGeneration(endPoint);
                _logger.LogInformation("RedisNearCache forgot endpoint {EndPoint} (was {State})", endPoint, wasArmed ? "armed" : "lost");
                Raise(EndpointRemoved, endPoint, nameof(EndpointRemoved));
            }
        }

        if (dropped is not null) FireAndForgetDispose(dropped);
    }

    // --- invalidation ---------------------------------------------------------------------------------

    /// <summary>
    /// One push frame from a broadcast socket. <c>["invalidate", [key, ...]]</c> is one event per key;
    /// <c>["invalidate", null]</c> is a <c>FLUSHDB</c>/<c>FLUSHALL</c>. Runs on the socket's read loop, so it
    /// must never throw: a handler that does would otherwise take the connection down with it.
    /// </summary>
    private void OnPush(EndPoint endPoint, Resp3Value push)
    {
        try
        {
            if (Volatile.Read(ref _disposed) == 1) return;
            if (push.Items.Count == 0) return;
            if (!string.Equals(push.Items[0].Text, "invalidate", StringComparison.OrdinalIgnoreCase))
            {
                _logger.LogDebug("RedisNearCache ignored a {Type} push from {EndPoint}", push.Items[0].Text ?? push.Items[0].Kind.ToString(), endPoint);
                return;
            }

            if (push.Items.Count < 2 || push.Items[1].IsNull)
            {
                _logger.LogDebug("RedisNearCache received a null invalidation from {EndPoint} (FLUSHDB/FLUSHALL)", endPoint);
                RaiseFlushAll();
                return;
            }

            var payload = push.Items[1];
            var trace = _logger.IsEnabled(LogLevel.Trace); // the params overload allocates per key otherwise
            foreach (var item in payload.Items)
            {
                if (item.Text is not { Length: > 0 } key) continue;
                if (trace) _logger.LogTrace("RedisNearCache received an invalidation for {Key} from {EndPoint}", key, endPoint);
                Raise(KeyInvalidated, key, nameof(KeyInvalidated));
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "RedisNearCache failed to handle an invalidation push from {EndPoint}", endPoint);
        }
    }

    private void RaiseFlushAll()
    {
        var handler = FlushAll;
        if (handler is null) return;
        try
        {
            handler();
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "RedisNearCache {EventName} handler threw", nameof(FlushAll));
        }
    }

    // --- disposal -------------------------------------------------------------------------------------

    /// <inheritdoc />
    /// <remarks>Closing a socket is what turns its tracking off, so no <c>CLIENT TRACKING OFF</c> is sent. No events are raised.</remarks>
    public async ValueTask DisposeAsync()
    {
        // Under the lifecycle lock, like Publish's disposed check: a socket armed concurrently is either installed
        // before this drain sees it, or refused by Publish. Neither leaks.
        List<KeyValuePair<EndPoint, Resp3Connection>> sockets;
        lock (_lifecycle)
        {
            if (Interlocked.Exchange(ref _disposed, 1) == 1) return;
            sockets = _sockets.ToList();
            _sockets.Clear();
        }

        UnhookEvents();
        try
        {
            await _shutdown.CancelAsync().ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            _logger.LogDebug(ex, "RedisNearCache ignored an error cancelling the broadcast tracker");
        }

        // Concurrently: each close waits up to a second for its read loop, and a cluster has several sockets.
        await Task.WhenAll(sockets.Select(async pair =>
        {
            try
            {
                await pair.Value.DisposeAsync().ConfigureAwait(false);
                _logger.LogDebug("RedisNearCache closed the broadcast socket to {EndPoint}", pair.Key);
            }
            catch (Exception ex)
            {
                _logger.LogDebug(ex, "RedisNearCache could not close the broadcast socket to {EndPoint} while disposing", pair.Key);
            }
        })).ConfigureAwait(false);

        _lost.Clear();
        _generations.Clear();
        foreach (var gate in _gates.Values) gate.Dispose();
        _gates.Clear();
        _shutdown.Dispose();
    }

    // --- helpers --------------------------------------------------------------------------------------

    /// <summary>
    /// Normalises the configured prefixes: duplicates removed, and any prefix already covered by a shorter one
    /// dropped, because Redis rejects a single client whose tracking prefixes overlap each other.
    /// </summary>
    internal static string[] NormalisePrefixes(IEnumerable<string>? configured, out string[] dropped)
    {
        dropped = [];
        if (configured is null) return [];

        var candidates = configured
            .Where(static p => p is not null)
            .Distinct(StringComparer.Ordinal)
            .OrderBy(static p => p.Length)
            .ThenBy(static p => p, StringComparer.Ordinal)
            .ToArray();

        var kept = new List<string>(candidates.Length);
        var covered = new List<string>();
        foreach (var candidate in candidates)
        {
            if (kept.Any(k => candidate.StartsWith(k, StringComparison.Ordinal))) covered.Add(candidate);
            else kept.Add(candidate);
        }

        dropped = covered.ToArray();
        return kept.ToArray();
    }

    /// <summary>The value of a named field of a <c>HELLO</c>-style map reply (the socket always speaks RESP3).</summary>
    private static Resp3Value? Field(Resp3Value reply, string name) => reply.MapValue(name);

    /// <summary>The <c>flags</c> of a <c>CLIENT TRACKINGINFO</c> reply, whose value is a set on RESP3.</summary>
    private static string[] ReadFlags(Resp3Value info)
    {
        if (Field(info, "flags") is not { } flags) return [];
        if (flags.Items.Count > 0) return flags.Items.Select(static f => f.Text ?? string.Empty).ToArray();
        return flags.Text is { Length: > 0 } single ? [single] : [];
    }

    /// <summary>The shutdown token, or false once the tracker is disposed (the source is disposed with it).</summary>
    private bool TryGetShutdownToken(out CancellationToken token)
    {
        try
        {
            token = _shutdown.Token;
            return true;
        }
        catch (ObjectDisposedException)
        {
            token = CancellationToken.None;
            return false;
        }
    }

    private long NextGeneration(EndPoint endPoint) => _generations.AddOrUpdate(endPoint, 1, static (_, generation) => generation + 1);

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

    private void FireAndForgetDispose(Resp3Connection socket) =>
        _ = Task.Run(async () => await DisposeSocketAsync(socket).ConfigureAwait(false), CancellationToken.None);

    private async Task DisposeSocketAsync(Resp3Connection socket)
    {
        try
        {
            await socket.DisposeAsync().ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            _logger.LogDebug(ex, "RedisNearCache ignored an error closing the broadcast socket to {Name}", socket.Name);
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
}
