using System.Net;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using RedisNearCache.Internal;
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
    private readonly RedisNearCacheOptions _options;
    private readonly ILogger<RedisNearCache> _logger;

    private readonly L1Cache _l1;
    private readonly InFlightTracker _inflight;
    private volatile bool _degraded;
    private int _startupSettled;

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
    /// Set once the server has rejected PTTL outright (an ACL without it, a proxy): the TTL cap cannot be honoured, so
    /// misses fall back to a plain GET and are cached for L1MaxAge, as with RespectServerTtl off. Logged once.
    /// </summary>
    private int _ttlCapUnavailable;

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
        _logger = logger;

        _l1 = new L1Cache(_options);
        _inflight = new InFlightTracker();

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
            await _armer.StartAsync(CancellationToken.None).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "RedisNearCache failed to start tracking; the near cache will not be coherent.");
            throw;
        }
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

    /// <summary>The one whole-cache flush. Every path that cannot trust L1 comes through here.</summary>
    private void FlushLocal()
    {
        _inflight.MarkAllInvalidated();
        _options.TestHooks.InsideFlushHandler?.Invoke();
        _l1.Clear();
        Statistics.Flush();
    }

    private void OnFlushAll() => FlushLocal();

    private void OnArmed(TrackingArmedEvent e)
    {
        // Initial: nothing was cached yet. Promoted: the node was armed while it was still a replica, so every read
        // routed to it since is tracked and no re-arm is needed; L1 is still flushed once, because entries read from
        // the demoted master before the failover are protected only by that node's tracking table from here on.
        if (e.Reason != ArmReason.Initial) FlushLocal();
        if (e.Reason is not (ArmReason.Initial or ArmReason.Promoted)) Statistics.Rearm();
        // Re-enable caching only AFTER the flush, so no concurrent read can hit an entry the flush discards.
        RemoveLost(e.EndPoint);
        // An arm succeeded, so startup (or its recovery) is settled and the cache is no longer degraded.
        Volatile.Write(ref _startupSettled, 1);
        _degraded = false;
        SignalCoherence();
    }

    private void OnTrackingLost(EndPoint endPoint)
    {
        // Nothing can be trusted while tracking is down on any endpoint: stop populating L1 first, then flush.
        // The coherence source is reset BEFORE the loss is visible, so a waiter can never observe "not coherent"
        // against a still-completed source and spin.
        ResetCoherence();
        AddLost(endPoint);
        FlushLocal();
    }

    private void OnEndpointRemoved(EndPoint endPoint)
    {
        // The node is no longer a master of the deployment (demoted, failed over, or gone). Entries read from it are
        // protected by nothing now - its tracking may already be gone without an invalidation ever arriving - so
        // flush unconditionally, even if tracking was never reported lost there. Then stop waiting on it: it will
        // never be re-armed, so it must not keep us in pass-through. Flush first, re-enable after, as in OnArmed.
        FlushLocal();
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

    /// <summary>L1 may only be read or populated while tracking is believed to be armed everywhere.</summary>
    /// <remarks>
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
    private bool CachingEnabled => !_degraded && _lostCount == 0;

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
            if (IsCoherent) _coherence.TrySetResult();
        }
    }

    /// <summary>Makes the next wait pend again. Called before every event that takes coherence away.</summary>
    private void ResetCoherence()
    {
        lock (_coherenceLock)
        {
            if (_coherence.Task.IsCompleted) _coherence = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        }
    }

    private void ThrowIfDisposed()
    {
        if (Volatile.Read(ref _disposed) == 1) throw new ObjectDisposedException(nameof(RedisNearCache));
    }

    private bool MatchesPrefixes(string key)
    {
        var prefixes = _options.KeyPrefixes;
        if (prefixes.Count == 0)
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
        ThrowIfDisposed();
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

            // Settle exactly once. A faulted or cancelled startup degrades to a pass-through (every read goes
            // to Redis, nothing is cached) until the armer's background retry raises Armed, which clears the
            // flag and settles startup itself. Never re-derive the flag from Ready on later calls: Ready stays
            // faulted forever, but the cache does not.
            if (Interlocked.Exchange(ref _startupSettled, 1) == 0 && !Ready.IsCompletedSuccessfully)
            {
                ResetCoherence();
                _degraded = true;
            }
        }

        if (CachingEnabled && _l1.TryGet(key, out var cached))
        {
            Statistics.Hit();
            return _options.Serializer.Deserialize<T>(cached);
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
            if (!cacheable && CachingEnabled && Volatile.Read(ref _untrackedReadsUnavailable) == 0)
            {
                // Outside KeyPrefixes: nothing will be stored, so do not let the server track the key either.
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
                    // The value is good; only the cap is unknown (a timeout, a dropped connection). Serve it, store nothing.
                    _logger.LogDebug(ex, "RedisNearCache could not read PTTL for {Key}; the value is served but not cached", key);
                    ttlUnknown = true;
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
                return default;
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

            return _options.Serializer.Deserialize<T>(bytes);
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
        byte[] bytes = _options.Serializer.Serialize(value);
        var db = _connection.Multiplexer.GetDatabase();
        // Tracking is armed with NOLOOP, so the server will not echo this write back as an invalidation.
        // Any read of this key in flight before or during the write must therefore be discarded by us:
        // mark before the write (reads already on the wire) and after it (reads that raced the send).
        InvalidateLocal(key);
        try
        {
            await db.StringSetAsync(key, bytes, expiry, When.Always).WaitAsync(cancellationToken).ConfigureAwait(false);
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

    public void EvictLocal(string key) => _l1.Remove(key);

    public void EvictAllLocal() => FlushLocal();

    public bool TryGetLocal<T>(string key, out T? value)
    {
        if (CachingEnabled && Volatile.Read(ref _disposed) == 0 && _l1.TryGet(key, out var bytes))
        {
            value = _options.Serializer.Deserialize<T>(bytes);
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

        // Disposing the armer cancels any arm in flight, so a startup racing this dispose finishes promptly.
        await _armer.DisposeAsync().ConfigureAwait(false);
        try { await Ready.ConfigureAwait(false); } catch { /* faulted or cancelled startup is fine here */ }
        await _listener.DisposeAsync().ConfigureAwait(false);
        _l1.Dispose();
        await _connection.DisposeAsync().ConfigureAwait(false);
        // Waiters can never be satisfied now; wake them so they observe the disposal.
        lock (_coherenceLock) _coherence.TrySetResult();
    }
}
