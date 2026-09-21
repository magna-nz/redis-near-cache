using System.Buffers;
using System.Diagnostics;
using System.Net;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using RedisNearCache.Internal;
using RedisNearCache.Tracking;
using StackExchange.Redis;

namespace RedisNearCache.Caching;

/// <summary>
/// The public facade: L1 (<see cref="L1Cache"/>) in front of Redis, kept coherent by
/// <see cref="ITrackingArmer"/> and <see cref="IInvalidationListener"/>.
/// </summary>
internal sealed class RedisNearCache : IRedisNearCache
{
    private readonly RedisNearCacheConnection _connection;
    private readonly ITrackingArmer _armer;
    private readonly IInvalidationListener _listener;
    private readonly string[] _keyPrefixes;
    private readonly string? _keyNamespace;
    private readonly RedisNearCacheOptions _options;
    private readonly ILogger<RedisNearCache> _logger;

    private readonly L1Cache _l1;
    private readonly InFlightTracker _inflight;
    private readonly RedisNearCacheMetrics _metrics;
    private volatile bool _degraded;
    private int _startupSettled;

    /// <summary>
    /// True until the start sequence has run to its end, background retry included. Nothing is cached meanwhile:
    /// the masters are armed concurrently and each raises its own <see cref="ArmReason.Initial"/> <c>Armed</c>, which
    /// says nothing about the others, and an initial arm never announces a loss first. A read let through by the
    /// first of them could be routed to a master whose <c>CLIENT TRACKING ON</c> has not been sent yet and be stored
    /// untracked, with no flush to follow. See <see cref="CachingEnabled"/>.
    /// </summary>
    private volatile bool _starting = true;

    /// <summary>
    /// Orders "the start failed" against an <c>Armed</c> raised by a recovery that beat it (the armer's retry loop or
    /// its reconcile): whichever comes second must not leave the cache degraded with nothing left to clear it.
    /// </summary>
    private readonly object _startLock = new();
    private bool _armedOutsideStart; // guarded by _startLock

    /// <summary>Stops the background retry of a start whose subscription failed. Cancelled on dispose.</summary>
    private readonly CancellationTokenSource _startRetry = new();

    /// <summary>
    /// Endpoints whose tracking is lost: added on <see cref="ITrackingArmer.TrackingLost"/>, removed on
    /// <see cref="ITrackingArmer.Armed"/> or <see cref="ITrackingArmer.EndpointRemoved"/>. Mutated only under
    /// <see cref="_lostLock"/>, and only through <see cref="AddLost"/> and <see cref="RemoveLost"/>, which publish its
    /// size to <see cref="_lostCount"/> before releasing the lock. See <see cref="CachingEnabled"/>.
    /// </summary>
    private readonly HashSet<EndPoint> _lostEndpoints = new();
    private readonly object _lostLock = new();
    private volatile int _lostCount;

    private int _disposed;

    /// <summary>
    /// Completed while the cache is coherent; replaced by a fresh, pending source whenever coherence is lost.
    /// <see cref="WaitForCoherenceAsync"/> re-checks <see cref="IsCoherent"/> after every completion, so a source
    /// that completes and is immediately superseded never lets a waiter through wrongly.
    /// </summary>
    private TaskCompletionSource _coherence = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private readonly object _coherenceLock = new();

    /// <summary>
    /// <see cref="Stopwatch.GetTimestamp"/> of the moment the cache last stopped being coherent, or
    /// <see cref="NotInPassThrough"/> while it is. A monotonic clock, not <c>DateTime.UtcNow</c>: the value is only
    /// ever subtracted from a later reading of the same clock, which a wall clock adjustment would corrupt.
    /// <para>
    /// Initialised to "now" because a new cache IS in pass-through - <see cref="_starting"/> is true until the start
    /// sequence returns - so an instance that never arms reports a growing duration rather than a healthy-looking 0.
    /// </para>
    /// Written only under <see cref="_coherenceLock"/>: set (if not already set) in <see cref="ResetCoherence"/>,
    /// which every path that takes coherence away calls first, and cleared in <see cref="SignalCoherence"/> under the
    /// same <c>IsCoherent</c> test that releases the waiters, so the timestamp and the coherence signal cannot
    /// disagree. Keyed off those two seams rather than off <see cref="_lostCount"/>, because a lost endpoint is only
    /// one of the three things <see cref="CachingEnabled"/> tests: a failed start degrades the cache with an EMPTY
    /// lost set, and the start itself is a pass-through stretch no loss is ever announced for.
    /// </summary>
    private long _passThroughSince = Stopwatch.GetTimestamp();

    /// <summary>Sentinel for "coherent, so there is no pass-through to time". 0 is a legitimate timestamp, so it cannot be used.</summary>
    private const long NotInPassThrough = long.MinValue;

    /// <summary>
    /// Set once the server has rejected PTTL outright (an ACL without it, a proxy): the TTL cap cannot be honoured, so
    /// misses fall back to a plain GET and are cached for L1MaxAge, as with RespectServerTtl off. Logged once.
    /// </summary>
    private int _ttlCapUnavailable;

    /// <summary>Set once the first TTL read failed on both the raw and the typed command, so the warning is said once.</summary>
    private int _ttlCapWarned;
    private int _namespacedKeyWarned;

    /// <summary>
    /// Consecutive misses whose TTL could not be read at all. Reset by the first success. A cap that cannot be read
    /// means the value is served but not stored, so a lasting failure would otherwise turn the cache off for those keys
    /// silently and indefinitely; at <see cref="TtlFailuresBeforeGivingUpTheCap"/> the cap is abandoned instead, which
    /// is the documented behaviour when a server refuses <c>PTTL</c>.
    /// </summary>
    private int _consecutiveTtlFailures;

    /// <summary>How many misses in a row may fail to read a TTL before the cap is abandoned rather than the cache.</summary>
    private const int TtlFailuresBeforeGivingUpTheCap = 3;

    /// <summary>
    /// Set once the server has rejected CLIENT CACHING inside a transaction (EXECABORT): untracked reads are not
    /// possible there, so keys outside KeyPrefixes are read with a plain, tracked GET, as before 0.5.2. Logged once.
    /// </summary>
    private int _untrackedReadsUnavailable;

    public RedisNearCacheStatistics Statistics { get; } = new();

    public Task Ready { get; }

    public RedisNearCache(
        RedisNearCacheConnection connection,
        ITrackingArmer armer,
        IInvalidationListener listener,
        IOptions<RedisNearCacheOptions> options,
        ILogger<RedisNearCache> logger)
    {
        _connection = connection;
        _armer = armer;
        _listener = listener;
        _options = options.Value;
        // Read once: the broadcast tracker arms the server with this same set at start, and the two must agree.
        _keyPrefixes = _options.EffectiveKeyPrefixes();
        _keyNamespace = string.IsNullOrEmpty(_options.KeyNamespace) ? null : _options.KeyNamespace;
        _logger = logger;

        _l1 = new L1Cache(_options);
        _inflight = new InFlightTracker();
        // The statistics object does not own L1, so it is pointed at it here and detached again on dispose.
        Statistics.AttachL1(() => _l1.Count);
        // Same arrangement for everything computed rather than counted: read when something collects, never on a
        // read. _passThroughSince, _lostCount and the two latches are state the facade keeps anyway; L1's refusal
        // count and the armer's pre-arm failure count belong to objects the statistics do not own either.
        Statistics.AttachFacade(
            () => PassThroughSeconds,
            () => _lostCount,
            () => Volatile.Read(ref _ttlCapUnavailable) == 1,
            () => Volatile.Read(ref _untrackedReadsUnavailable) == 1,
            () => _l1.StoreRefusals,
            () => _armer.PreArmFailures);
        // Observable instruments only: they read what is counted anyway, so no metric costs the read path anything.
        _metrics = new RedisNearCacheMetrics(Statistics, _connection.ClientName, () => IsCoherent, _options.InstanceName);

        _listener.KeyInvalidated += OnKeyInvalidated;
        _listener.FlushAll += OnFlushAll;
        _armer.Armed += OnArmed;
        _armer.TrackingLost += OnTrackingLost;
        _armer.EndpointRemoved += OnEndpointRemoved;

        Ready = StartAsync();
    }

    private async Task StartAsync()
    {
        try
        {
            // Listener first: the subscriber connection must exist and be subscribed before tracking is
            // armed, otherwise an invalidation could be redirected before anyone is listening for it.
            await _listener.StartAsync(CancellationToken.None).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "RedisNearCache failed to start tracking; the near cache will not be coherent.");
            // The armer was never started, so it has hooked no event and runs no loop: nothing would ever arm this
            // cache. Keep trying here. (Broadcast: listener and armer are one object, whose failed start is memoised
            // and which recovers through its own sweep.)
            if (ReferenceEquals(_listener, _armer)) StartFailed();
            else RetryStartInBackground();
            throw;
        }

        try
        {
            await _armer.StartAsync(CancellationToken.None).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "RedisNearCache failed to start tracking; the near cache will not be coherent.");
            StartFailed();
            throw;
        }

        StartSucceeded();
    }

    /// <summary>
    /// The armer's start returned: every master that was connected is armed or announced lost, so from here on the
    /// lost set alone decides whether L1 may be used.
    /// </summary>
    private void StartSucceeded()
    {
        _starting = false;
        Volatile.Write(ref _startupSettled, 1);
        SignalCoherence();
    }

    /// <summary>
    /// The armer's start failed: nothing is armed (it found no connected master, or could arm none). Pass-through
    /// until the armer's own retry loops or reconcile raise <c>Armed</c>, which clears the flag. Every master those
    /// paths arm was announced lost first, so the lost set covers the ones still to come.
    /// </summary>
    private void StartFailed()
    {
        lock (_startLock)
        {
            if (!_armedOutsideStart)
            {
                ResetCoherence();
                _degraded = true;
            }

            Volatile.Write(ref _startupSettled, 1);
        }

        _starting = false;
        SignalCoherence();
    }

    /// <summary>
    /// The subscription could not be made (Redis unreachable while the application started, with
    /// <c>abortConnect=false</c>). Retry it every few seconds, then start the armer for the first time. <see cref="Ready"/>
    /// stays faulted; the cache stays in pass-through until this succeeds, and <see cref="IsCoherent"/> says so.
    /// </summary>
    private void RetryStartInBackground()
    {
        var token = _startRetry.Token;
        var interval = _options.TestHooks.StartRetryInterval ?? TrackingRetry.SlowInterval;
        _ = Task.Run(async () =>
        {
            try
            {
                while (true)
                {
                    await Task.Delay(interval, token).ConfigureAwait(false);
                    try
                    {
                        await _listener.StartAsync(token).ConfigureAwait(false);
                        break;
                    }
                    catch (OperationCanceledException) when (token.IsCancellationRequested)
                    {
                        throw;
                    }
                    catch (Exception ex)
                    {
                        _logger.LogDebug(ex, "RedisNearCache still cannot subscribe to the invalidation channel; retrying in {Interval}", interval);
                    }
                }

                _logger.LogInformation("RedisNearCache subscribed to the invalidation channel after a failed start; arming CLIENT TRACKING");
                await _armer.StartAsync(token).ConfigureAwait(false);
                StartSucceeded();
            }
            catch (OperationCanceledException) when (token.IsCancellationRequested)
            {
                // disposed
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "RedisNearCache could not arm CLIENT TRACKING after a failed start; the armer keeps retrying in the background");
                StartFailed();
            }
        }, CancellationToken.None);
    }

    // Ordering rule: mark the in-flight tracker FIRST, then touch L1. A read that stores its reply between the
    // two steps re-checks the tracker after storing (see GetAsync), so mark-then-clear closes every
    // interleaving; clear-then-mark leaves the "store after clear, re-check before mark" window open.

    private void OnKeyInvalidated(string key)
    {
        _inflight.MarkInvalidated(key);
        _l1.Remove(key);
        Statistics.Invalidation();
    }

    /// <summary>
    /// The one whole-cache flush. Every path that cannot trust L1 comes through here. <paramref name="reason"/> is
    /// counted alongside the total and published as the <c>reason</c> tag; it changes nothing about the order of the
    /// four steps below, which is load-bearing (DESIGN.md) and which other tests read the counter to observe.
    /// </summary>
    private void FlushLocal(FlushReason reason)
    {
        _inflight.MarkAllInvalidated();
        _options.TestHooks.InsideFlushHandler?.Invoke();
        _l1.Clear();
        Statistics.Flush(reason);
    }

    private void OnFlushAll() => FlushLocal(FlushReason.ServerFlush);

    private void OnArmed(TrackingArmedEvent e)
    {
        // Initial: nothing was cached yet. Promoted: the node was armed while it was still a replica, so every read
        // routed to it since is tracked and no re-arm is needed; L1 is still flushed once, because entries read from
        // the demoted master before the failover are protected only by that node's tracking table from here on.
        if (e.Reason != ArmReason.Initial) FlushLocal(FlushReason.Rearm);
        if (e.Reason is not (ArmReason.Initial or ArmReason.Promoted)) Statistics.Rearm(e.Reason);
        // Re-enable caching only AFTER the flush, so no concurrent read can hit an entry the flush discards.
        RemoveLost(e.EndPoint);
        // One master's initial arm says nothing about the others the same start is still arming, none of which was
        // announced lost: the start sequence settles startup itself, once all of them are done (see _starting).
        if (e.Reason == ArmReason.Initial && _starting) return;
        // Any other arm succeeded, so startup (or its recovery) is settled and the cache is no longer degraded.
        lock (_startLock)
        {
            _armedOutsideStart = true;
            Volatile.Write(ref _startupSettled, 1);
            _degraded = false;
        }

        SignalCoherence();
    }

    private void OnTrackingLost(EndPoint endPoint)
    {
        // Nothing can be trusted while tracking is down on any endpoint: stop populating L1 first, then flush.
        // The coherence source is reset BEFORE the loss is visible, so a waiter can never observe "not coherent"
        // against a still-completed source and spin.
        ResetCoherence();
        AddLost(endPoint);
        FlushLocal(FlushReason.TrackingLost);
    }

    private void OnEndpointRemoved(EndPoint endPoint)
    {
        // The node is no longer a master of the deployment (demoted, failed over, or gone). Entries read from it are
        // protected by nothing now - its tracking may already be gone without an invalidation ever arriving - so
        // flush unconditionally, even if tracking was never reported lost there. Then stop waiting on it: it will
        // never be re-armed, so it must not keep us in pass-through. Flush first, re-enable after, as in OnArmed.
        FlushLocal(FlushReason.EndpointRemoved);
        RemoveLost(endPoint);
        SignalCoherence();
    }

    private void AddLost(EndPoint endPoint)
    {
        lock (_lostLock)
        {
            if (_lostEndpoints.Add(endPoint)) _lostCount = _lostEndpoints.Count;
        }
    }

    private void RemoveLost(EndPoint endPoint)
    {
        lock (_lostLock)
        {
            if (_lostEndpoints.Remove(endPoint)) _lostCount = _lostEndpoints.Count;
        }
    }

    /// <summary>
    /// The addresses of the endpoints whose tracking is lost, comma-joined, or an empty string when none is. For the
    /// health check's <c>Data</c> and for logs only: an endpoint address is unbounded cardinality, so it must never
    /// become a metric tag, which is why <see cref="RedisNearCacheStatistics.LostEndpointCount"/> is the number alone.
    /// Takes <see cref="_lostLock"/>, so it is a diagnostic read, not something to do per request.
    /// </summary>
    internal string LostEndpointAddresses()
    {
        lock (_lostLock)
        {
            return _lostEndpoints.Count == 0 ? string.Empty : string.Join(",", _lostEndpoints.Select(e => e.ToString()));
        }
    }

    /// <summary>
    /// Seconds since coherence was last lost, or 0 while the cache is coherent. Computed from
    /// <see cref="_passThroughSince"/> at collection time; nothing on the read path maintains it.
    /// </summary>
    private double PassThroughSeconds
    {
        get
        {
            var since = Volatile.Read(ref _passThroughSince);
            if (since == NotInPassThrough) return 0;
            var elapsed = Stopwatch.GetTimestamp() - since;
            // A clear racing this read (the cache just became coherent) can make the difference negative; report the
            // coherent answer rather than a negative duration.
            return elapsed <= 0 ? 0 : (double)elapsed / Stopwatch.Frequency;
        }
    }

    /// <summary>L1 may only be read or populated while tracking is believed to be armed everywhere.</summary>
    /// <remarks>
    /// <para>
    /// "Everywhere" is what the lost set cannot say while the cache is starting: an initial arm announces no loss, so
    /// the set is empty although masters are still unarmed. <see cref="_starting"/> covers that stretch.
    /// </para>
    /// Evaluated on every read, L1 hits included, so it must not lock: <c>ConcurrentDictionary.IsEmpty</c>, used here
    /// before, takes every one of its locks when the dictionary is empty, which is the steady state.
    /// <para>
    /// Invariant: whenever <see cref="_lostLock"/> is free, <c>_lostCount == _lostEndpoints.Count</c>. Both mutators
    /// hold the lock, change the set, and write the count from the set's own size before releasing it, so the count is
    /// never derived from a count of events. A read sees the last completed write (an int write is atomic; the field
    /// is volatile), so <c>_lostCount == 0</c> means the set was empty at the most recent add or remove to complete.
    /// The count write is the moment a loss becomes, or stops being, visible to reads; the handlers place it exactly
    /// where the old dictionary write was, so every ordering rule is unchanged:
    /// </para>
    /// <list type="bullet">
    /// <item>Loss: the count is non-zero before <see cref="FlushLocal"/> starts. A read that checked just before the
    /// write either served an entry (it linearizes before the loss) or is in flight and started before the flush's
    /// <c>MarkAllInvalidated</c>, so its store is discarded by the in-flight re-check.</item>
    /// <item>Arm and removal: the count drops only after <see cref="FlushLocal"/> returns, so no read is served an
    /// entry that flush is about to discard.</item>
    /// <item>Duplicate <c>TrackingLost</c> for one endpoint: the second <c>Add</c> returns false and writes nothing.
    /// One <c>Armed</c> or <c>EndpointRemoved</c> clears it; an event counter would wait for a second one forever.</item>
    /// <item><c>Armed</c> or <c>EndpointRemoved</c> for an endpoint that is not lost (every Initial arm, a Promoted
    /// arm, a removal of an armed node): <c>Remove</c> returns false and writes nothing. An event counter would go
    /// negative and report zero while a later loss is still in the set.</item>
    /// <item>Removal racing re-arm of the same lost endpoint: both flush, then both remove; one <c>Remove</c> succeeds
    /// and writes the size, the other is a no-op. The endpoint ends not lost whichever runs first, as before.</item>
    /// <item>Events for different endpoints on different threads: the lock serializes the set, and each write reflects
    /// the set after its own change, so no update is lost and a zero is only written when the set is empty.</item>
    /// </list>
    /// <para>
    /// Not covered here, and not before: a <c>TrackingLost</c> and an <c>Armed</c>/<c>EndpointRemoved</c> for the SAME
    /// endpoint on different threads, where the clear's <c>Remove</c> lands after the loss's <c>Add</c> although the
    /// loss was the later event. The facade's result follows whichever mutation lands last. <see cref="Tracking.TrackingArmer"/>
    /// excludes it by raising all three events under its lifecycle lock (DESIGN.md, Endpoint lifecycle), so the facade
    /// sees one endpoint's events in the armer's order.
    /// </para>
    /// </remarks>
    private bool CachingEnabled => !_starting && !_degraded && _lostCount == 0;

    /// <inheritdoc />
    public bool IsCoherent => Volatile.Read(ref _startupSettled) == 1 && CachingEnabled && Volatile.Read(ref _disposed) == 0;

    /// <inheritdoc />
    public async Task WaitForCoherenceAsync(CancellationToken cancellationToken = default)
    {
        while (true)
        {
            ThrowIfDisposed();
            if (IsCoherent) return;
            TaskCompletionSource pending;
            lock (_coherenceLock) pending = _coherence;
            if (pending.Task.IsCompleted)
            {
                // Coherence was lost after the source completed and before the losing path replaced it; give that
                // path a chance to run rather than spinning on a completed task.
                cancellationToken.ThrowIfCancellationRequested();
                await Task.Yield();
                continue;
            }
            await pending.Task.WaitAsync(cancellationToken).ConfigureAwait(false);
        }
    }

    /// <summary>Lets waiters through if the cache is coherent now. Called after every event that can restore coherence.</summary>
    private void SignalCoherence()
    {
        lock (_coherenceLock)
        {
            if (!IsCoherent) return;
            _coherence.TrySetResult();
            // Inside the same lock and under the same test as the signal, so the duration and the coherence state can
            // never disagree: whatever says "coherent" to a waiter says "0 seconds of pass-through" to a collector.
            Volatile.Write(ref _passThroughSince, NotInPassThrough);
        }
    }

    /// <summary>Makes the next wait pend again. Called before every event that takes coherence away.</summary>
    private void ResetCoherence()
    {
        lock (_coherenceLock)
        {
            // Regardless of the source's state, and only if the clock is not already running: a second loss while the
            // first is still pending must not restart the clock, or a flapping endpoint would report a duration that
            // never grows past the gap between two losses.
            if (Volatile.Read(ref _passThroughSince) == NotInPassThrough) Volatile.Write(ref _passThroughSince, Stopwatch.GetTimestamp());
            if (_coherence.Task.IsCompleted) _coherence = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        }
    }

    private void ThrowIfDisposed()
    {
        if (Volatile.Read(ref _disposed) == 1) throw new ObjectDisposedException(nameof(RedisNearCache));
    }

    /// <summary>
    /// The key as Redis, L1 and the in-flight tracker know it. Applied once, where a caller's key comes in; everything
    /// below that works on full keys, which is also what the server's invalidations carry, so that path never has to
    /// translate anything. Without a namespace the key is passed through untouched - not even checked, so nothing
    /// changes for a cache that has none. With one a null key must be refused: it would otherwise quietly become the
    /// namespace itself.
    /// </summary>
    private string FullKey(string key)
    {
        if (_keyNamespace is null) return key;
        ArgumentNullException.ThrowIfNull(key);

        // The mistake to expect from someone adopting KeyNamespace: still passing full keys. It fails quietly - the
        // key becomes "ns:ns:..." - so say so, once. A key may legitimately begin with the same text, hence a
        // warning and nothing more.
        if (Volatile.Read(ref _namespacedKeyWarned) == 0
            && key.StartsWith(_keyNamespace, StringComparison.Ordinal)
            && Interlocked.Exchange(ref _namespacedKeyWarned, 1) == 0)
        {
            _logger.LogWarning(
                "RedisNearCache was given the key {Key}, which already begins with its KeyNamespace {KeyNamespace}; the namespace is added to every key, so this one is read and written as {FullKey}. Pass keys without the namespace. This is reported once",
                key, _keyNamespace, _keyNamespace + key);
        }

        return _keyNamespace + key;
    }

    private bool MatchesPrefixes(string key)
    {
        var prefixes = _keyPrefixes;
        if (prefixes.Length == 0)
        {
            return true;
        }

        foreach (var prefix in prefixes)
        {
            if (key.StartsWith(prefix, StringComparison.Ordinal))
            {
                return true;
            }
        }

        return false;
    }

    public async ValueTask<T?> GetAsync<T>(string key, CancellationToken cancellationToken = default)
    {
        var bytes = await GetStoredBytesAsync(key, cancellationToken).ConfigureAwait(false);
        return bytes is null ? default : Deserialize<T>(bytes);
    }

    // The only two places the configured serializer is called. A try/catch that does not throw costs nothing, so
    // counting the failures adds nothing to the read or write path; the exception is rethrown untouched, neither
    // swallowed nor wrapped, so a caller still sees exactly what its serializer threw.
    private byte[] Serialize<T>(T value)
    {
        try
        {
            return _options.Serializer.Serialize(value);
        }
        catch
        {
            Statistics.SerializerFailure();
            throw;
        }
    }

    private T? Deserialize<T>(ReadOnlyMemory<byte> bytes)
    {
        try
        {
            return _options.Serializer.Deserialize<T>(bytes);
        }
        catch
        {
            Statistics.SerializerFailure();
            throw;
        }
    }

    // A copy: the array read may be the one held in L1.
    public async ValueTask<byte[]?> GetBytesAsync(string key, CancellationToken cancellationToken = default) =>
        (await GetStoredBytesAsync(key, cancellationToken).ConfigureAwait(false))?.ToArray();

    // Both multi-key reads are the interface's own fan-out over the single-key members above - there is no second
    // read path to keep in step with GetStoredBytesAsync. They are implemented here only so that a disposed cache
    // says so even for an empty key list, which never reaches a single-key read.
    public async ValueTask<IReadOnlyDictionary<string, T?>> GetManyAsync<T>(IEnumerable<string> keys, CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        return await ManyReads.ReadAsync<T?>(keys, (key, token) => GetAsync<T>(key, token), cancellationToken).ConfigureAwait(false);
    }

    public async ValueTask<IReadOnlyDictionary<string, byte[]?>> GetManyBytesAsync(IEnumerable<string> keys, CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        return await ManyReads.ReadAsync<byte[]?>(keys, (key, token) => GetBytesAsync(key, token), cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// The read behind <c>IBufferDistributedCache.TryGetAsync</c>: the same path as <see cref="GetBytesAsync"/>, but
    /// copying the stored bytes straight into <paramref name="destination"/> instead of into an intermediate array
    /// the caller would then copy again. The L1-held array is never handed out, only read from.
    /// </summary>
    internal async ValueTask<bool> TryWriteStoredBytesAsync(string key, IBufferWriter<byte> destination, CancellationToken cancellationToken)
    {
        var bytes = await GetStoredBytesAsync(key, cancellationToken).ConfigureAwait(false);
        if (bytes is null)
        {
            return false;
        }

        destination.Write(bytes);
        return true;
    }

    /// <summary>
    /// The read path shared by the typed and raw reads: the bytes as stored in Redis, or null when the key does not
    /// exist. The array returned may be the instance held in L1, so callers must not hand it out or change it.
    /// </summary>
    private async ValueTask<byte[]?> GetStoredBytesAsync(string key, CancellationToken cancellationToken)
    {
        ThrowIfDisposed();
        // Before L1 and before Redis: a caller that cancelled already must not be handed a locally cached value
        // either, or cancellation would be observed only on a miss.
        cancellationToken.ThrowIfCancellationRequested();
        key = FullKey(key);
        if (Volatile.Read(ref _startupSettled) == 0)
        {
            if (!Ready.IsCompleted)
            {
                try
                {
                    await Ready.WaitAsync(cancellationToken).ConfigureAwait(false);
                }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                {
                    throw;
                }
                catch
                {
                    // already logged by StartAsync; handled below
                }
            }

            // Nothing to settle here: the start sequence settles its own outcome before Ready completes (see
            // StartSucceeded and StartFailed). A faulted startup is a pass-through (every read goes to Redis, nothing
            // is cached) until an Armed clears it, or, where the subscription itself failed, until the background
            // retry of the start succeeds. Ready stays faulted forever; the cache does not.
        }

        if (CachingEnabled && _l1.TryGet(key, out var cached))
        {
            Statistics.Hit();
            return cached;
        }

        Statistics.Miss();
        var cacheable = MatchesPrefixes(key);
        long token = _inflight.Begin(key);
        try
        {
            var db = _connection.Multiplexer.GetDatabase();
            RedisValue value;
            long? ttlMilliseconds = null;
            var ttlUnknown = false; // PTTL failed: the value is good but must not be stored without its cap
            if (!cacheable && CachingEnabled && _options.TrackingMode == TrackingMode.Redirect && Volatile.Read(ref _untrackedReadsUnavailable) == 0)
            {
                // Outside KeyPrefixes: nothing will be stored, so do not let the server track the key either.
                // (Redirect mode only: in Broadcast mode the reading connection is not tracked at all, and the
                // server only pushes for keys under KeyPrefixes, so a plain GET is already untracked.)
                // The connection is armed in OPTOUT mode, and CLIENT CACHING NO applies to the next command on the
                // same connection. A MULTI/EXEC is the only way StackExchange.Redis writes two commands adjacently
                // on a multiplexed connection; the pair is atomic on the server, so no other read slips between.
                value = await ReadUntrackedAsync(db, key, cancellationToken).ConfigureAwait(false);
            }
            else if (cacheable && CachingEnabled && _options.RespectServerTtl && Volatile.Read(ref _ttlCapUnavailable) == 0)
            {
                // GET and PTTL pipelined back to back: one round trip. A write landing between them pushes an
                // invalidation for the key, which the in-flight tracker turns into a discard below, so a value from
                // before the write is never stored with a TTL from after it.
                var getTask = db.StringGetAsync(key);
                var ttlTask = db.ExecuteAsync("PTTL", (RedisKey)key);
                try
                {
                    value = await getTask.WaitAsync(cancellationToken).ConfigureAwait(false);
                }
                catch
                {
                    Observe(getTask);
                    Observe(ttlTask);
                    throw;
                }

                try
                {
                    ttlMilliseconds = (long)await ttlTask.WaitAsync(cancellationToken).ConfigureAwait(false);
                    Volatile.Write(ref _consecutiveTtlFailures, 0);
                }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                {
                    Observe(ttlTask);
                    throw;
                }
                catch (RedisServerException ex)
                {
                    // The server refuses PTTL on this connection: no cap will ever be available. Say so once and stop
                    // asking; from now on misses are plain GETs cached for L1MaxAge, as with RespectServerTtl off.
                    if (Interlocked.Exchange(ref _ttlCapUnavailable, 1) == 0)
                        _logger.LogWarning(ex, "RedisNearCache: the server rejected PTTL, so RespectServerTtl cannot be honoured; L1 entries are capped by L1MaxAge only from now on");
                    ttlUnknown = true;
                }
                catch (Exception ex)
                {
                    // The raw PTTL failed for a reason the server did not state: a timeout, a dropped connection, or a
                    // slot whose owner this command could not be routed to (a resharding cluster). The value is good,
                    // so try the typed TTL once before giving up on the cap: it is routed and redirected like any other
                    // keyed command, where a raw Execute is not. Without this a key can stay uncacheable for as long as
                    // the condition lasts, silently, because a read with no cap is served but never stored.
                    try
                    {
                        var typed = await db.KeyTimeToLiveAsync(key).WaitAsync(cancellationToken).ConfigureAwait(false);
                        // Null is "no expiry" here, and also the (already invalidated) key that vanished meanwhile: the
                        // in-flight check below discards that reply, so storing it uncapped is not a staleness window.
                        ttlMilliseconds = typed is { } remaining ? (long)remaining.TotalMilliseconds : -1;
                        Volatile.Write(ref _consecutiveTtlFailures, 0);
                        _logger.LogDebug(ex, "RedisNearCache could not read PTTL for {Key} directly; the typed TTL answered instead", key);
                    }
                    catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                    {
                        throw;
                    }
                    catch (Exception fallback)
                    {
                        // Both failed: serve the value, store nothing. Said once at Warning rather than per read, because
                        // a lasting failure here means this instance caches nothing while looking healthy.
                        if (Interlocked.Exchange(ref _ttlCapWarned, 1) == 0)
                            _logger.LogWarning(fallback, "RedisNearCache cannot read the TTL of {Key} ({Reason}), so its value is served but not cached; this is reported once", key, ex.Message);
                        else
                            _logger.LogDebug(fallback, "RedisNearCache could not read the TTL of {Key}; the value is served but not cached", key);
                        ttlUnknown = true;

                        // Neither command can be answered. If that keeps happening, give up the cap rather than the
                        // cache: an entry stored for L1MaxAge is what RespectServerTtl=false does, and an invalidation
                        // still evicts it, whereas storing nothing at all leaves this instance reading through to Redis
                        // for as long as the condition lasts, with nothing in the statistics to show for it.
                        if (Interlocked.Increment(ref _consecutiveTtlFailures) >= TtlFailuresBeforeGivingUpTheCap
                            && Interlocked.Exchange(ref _ttlCapUnavailable, 1) == 0)
                        {
                            _logger.LogWarning(fallback,
                                "RedisNearCache could not read a TTL on {Failures} misses in a row, so RespectServerTtl is abandoned: L1 entries are capped by L1MaxAge only from now on",
                                TtlFailuresBeforeGivingUpTheCap);
                        }
                    }
                }
            }
            else
            {
                value = await db.StringGetAsync(key).WaitAsync(cancellationToken).ConfigureAwait(false);
            }

            var afterRead = _options.TestHooks.AfterRedisReadBeforeStore;
            if (afterRead is not null)
            {
                await afterRead(key).ConfigureAwait(false);
            }

            byte[]? bytes = (byte[]?)value;
            if (bytes is null)
            {
                return null;
            }

            // A read that started in pass-through has no TTL to cap by; if caching came back meanwhile, leave the entry
            // to the next read rather than store it for the full L1MaxAge against the option.
            if (CachingEnabled && cacheable && !ttlUnknown && (ttlMilliseconds is not null || !_options.RespectServerTtl || Volatile.Read(ref _ttlCapUnavailable) == 1))
            {
                // PTTL: -1 no expiry, -2 the key vanished between GET and PTTL (expired or deleted; the server has
                // pushed, or is about to push, an invalidation for it), otherwise the remaining milliseconds.
                TimeSpan? cap = ttlMilliseconds switch
                {
                    null or -1 => null,
                    < 0 => TimeSpan.Zero,
                    long ms => TimeSpan.FromMilliseconds(ms),
                };

                if (cap == TimeSpan.Zero || _inflight.WasInvalidated(key, token))
                {
                    Statistics.RaceDiscard();
                }
                else
                {
                    _l1.Set(key, bytes, cap);
                    // Re-check after the store: an invalidation that landed between the check above and the
                    // Set would have found nothing to remove, so remove it ourselves.
                    if (_inflight.WasInvalidated(key, token))
                    {
                        _l1.Remove(key);
                        Statistics.RaceDiscard();
                    }
                }
            }

            return bytes;
        }
        finally
        {
            _inflight.End(key, token);
        }
    }

    /// <summary>
    /// <c>MULTI</c> / <c>CLIENT CACHING NO</c> / <c>GET</c> / <c>EXEC</c>. If the node is not in OPTOUT mode (tracking
    /// is off there while it is being re-armed) the CACHING command fails inside EXEC and the GET still answers; the
    /// key is then tracked as before, which is harmless. If the server refuses to even queue CLIENT CACHING (an ACL
    /// without it, a proxy) EXEC is aborted; that is reported once and every later read outside KeyPrefixes is a
    /// plain, tracked GET. A transaction that did not run at all also falls back to a plain GET.
    /// </summary>
    private async Task<RedisValue> ReadUntrackedAsync(IDatabase db, string key, CancellationToken cancellationToken)
    {
        var transaction = db.CreateTransaction();
        var caching = transaction.ExecuteAsync("CLIENT", "CACHING", "NO");
        var get = transaction.StringGetAsync(key);
        var exec = transaction.ExecuteAsync();
        bool executed;
        try
        {
            executed = await exec.WaitAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (RedisServerException ex)
        {
            Observe(caching);
            Observe(get);
            if (Interlocked.Exchange(ref _untrackedReadsUnavailable, 1) == 0)
                _logger.LogWarning(ex, "RedisNearCache: the server rejected CLIENT CACHING NO inside a transaction, so keys outside KeyPrefixes will be read with a plain GET and tracked from now on");
            return await db.StringGetAsync(key).WaitAsync(cancellationToken).ConfigureAwait(false);
        }
        catch
        {
            Observe(exec);
            Observe(caching);
            Observe(get);
            throw;
        }

        Observe(caching);
        if (!executed)
        {
            Observe(get);
            _logger.LogDebug("RedisNearCache untracked read of {Key} did not execute as a transaction; reading it tracked instead", key);
            return await db.StringGetAsync(key).WaitAsync(cancellationToken).ConfigureAwait(false);
        }

        return await get.WaitAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <summary>Marks a task's eventual failure as observed so an abandoned pipelined command never surfaces as an unobserved exception.</summary>
    private static void Observe(Task task)
    {
        if (task.IsCompleted)
        {
            _ = task.Exception;
            return;
        }

        _ = task.ContinueWith(static t => _ = t.Exception, CancellationToken.None, TaskContinuationOptions.OnlyOnFaulted | TaskContinuationOptions.ExecuteSynchronously, TaskScheduler.Default);
    }

    public async ValueTask SetAsync<T>(string key, T value, TimeSpan? expiry = null, CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        await WriteAsync(key, Serialize(value), expiry, When.Always, keepTtl: false, cancellationToken).ConfigureAwait(false);
    }

    public async ValueTask<bool> SetAsync<T>(string key, T value, When when, TimeSpan? expiry = null, bool keepTtl = false, CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        ConditionalWrite.ThrowIfInvalid(expiry, keepTtl);
        return await WriteAsync(key, Serialize(value), expiry, when, keepTtl, cancellationToken).ConfigureAwait(false);
    }

    public async ValueTask SetBytesAsync(string key, ReadOnlyMemory<byte> value, TimeSpan? expiry = null, CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        // Copied: a cancelled wait returns while the command may still be queued, and the caller's buffer (a pooled
        // one, from HybridCache) may be reused by then.
        await WriteAsync(key, value.ToArray(), expiry, When.Always, keepTtl: false, cancellationToken).ConfigureAwait(false);
    }

    public async ValueTask<bool> SetBytesAsync(string key, ReadOnlyMemory<byte> value, When when, TimeSpan? expiry = null, bool keepTtl = false, CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        ConditionalWrite.ThrowIfInvalid(expiry, keepTtl);
        // Copied for the same reason as the unconditional write above.
        return await WriteAsync(key, value.ToArray(), expiry, when, keepTtl, cancellationToken).ConfigureAwait(false);
    }

    private async ValueTask<bool> WriteAsync(string key, byte[] bytes, TimeSpan? expiry, When when, bool keepTtl, CancellationToken cancellationToken)
    {
        key = FullKey(key);
        var db = _connection.Multiplexer.GetDatabase();
        // In Redirect mode tracking is armed with NOLOOP, so the server does not echo this write back; in Broadcast
        // mode it does (the push connection never writes), and the echo is harmless: it evicts what we evict here.
        // Any read of this key in flight before or during the write must therefore be discarded by us:
        // mark before the write (reads already on the wire) and after it (reads that raced the send).
        // A conditional write (NX/XX) is treated no differently: whether it took is only known from the reply, so the
        // key is evicted either way. One that did not happen costs a local eviction, never a stale read.
        InvalidateLocal(key);
        try
        {
            return await db.StringSetAsync(key, bytes, expiry, keepTtl, when, CommandFlags.None).WaitAsync(cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            // Even if the reply never came back (timeout, cancellation) the write may have landed.
            InvalidateLocal(key);
        }
    }

    /// <summary>Marks the key for any read in flight, then drops the L1 copy. Order matters; see the note above the handlers.</summary>
    private void InvalidateLocal(string key)
    {
        _inflight.MarkInvalidated(key);
        _l1.Remove(key);
    }

    public async ValueTask<bool> RemoveAsync(string key, CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        key = FullKey(key);
        var db = _connection.Multiplexer.GetDatabase();
        InvalidateLocal(key);
        try
        {
            return await db.KeyDeleteAsync(key).WaitAsync(cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            InvalidateLocal(key);
        }
    }

    // Marked as well as removed: a read already on the wire would otherwise store its reply straight back.
    // A no-op once disposed, matching TryGetLocal: there is no L1 left to evict from, and reaching the disposed
    // MemoryCache would throw. Only these two public entry points are guarded - the invalidation handlers and the
    // write path call InvalidateLocal/FlushLocal directly and run only while the cache is alive.
    public void EvictLocal(string key)
    {
        if (Volatile.Read(ref _disposed) == 1) return;
        try { InvalidateLocal(FullKey(key)); }
        catch (ObjectDisposedException) { /* a dispose won the race after the check above: same no-op */ }
    }

    public void EvictAllLocal()
    {
        if (Volatile.Read(ref _disposed) == 1) return;
        try { FlushLocal(FlushReason.Manual); }
        catch (ObjectDisposedException) { /* a dispose won the race after the check above: same no-op */ }
    }

    public bool TryGetLocal<T>(string key, out T? value)
    {
        if (CachingEnabled && Volatile.Read(ref _disposed) == 0 && _l1.TryGet(FullKey(key), out var bytes))
        {
            value = Deserialize<T>(bytes);
            return true;
        }

        value = default;
        return false;
    }

    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) == 1)
        {
            return;
        }

        _listener.KeyInvalidated -= OnKeyInvalidated;
        _listener.FlushAll -= OnFlushAll;
        _armer.Armed -= OnArmed;
        _armer.TrackingLost -= OnTrackingLost;
        _armer.EndpointRemoved -= OnEndpointRemoved;
        // Before the armer goes: a background retry of the start must not start it after it was disposed.
        _startRetry.Cancel();

        try
        {
            // Disposing the armer cancels any arm in flight, so a startup racing this dispose finishes promptly.
            await _armer.DisposeAsync().ConfigureAwait(false);
            try { await Ready.ConfigureAwait(false); } catch { /* faulted or cancelled startup is fine here */ }
            await _listener.DisposeAsync().ConfigureAwait(false);
        }
        finally
        {
            // Even if the armer or listener threw: an undisposed Meter stays registered for the life of the process
            // and its callbacks would keep this cache, its L1 and its multiplexer reachable.
            // Stop publishing before L1 goes, so no collection cycle can reach a disposed store.
            _metrics.Dispose();
            Statistics.DetachL1();
            Statistics.DetachFacade();
            _l1.Dispose();
        }

        await _connection.DisposeAsync().ConfigureAwait(false);
        _startRetry.Dispose();
        // Waiters can never be satisfied now; wake them so they observe the disposal.
        lock (_coherenceLock) _coherence.TrySetResult();
    }
}
