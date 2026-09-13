namespace RedisNearCache;

/// <summary>Thread-safe counters exposed by <see cref="IRedisNearCache.Statistics"/>.</summary>
public sealed class RedisNearCacheStatistics
{
    private long _hits, _misses, _invalidations, _flushes, _rearms, _raceDiscards;

    /// <summary>Reads served from L1 without touching Redis.</summary>
    public long Hits => Volatile.Read(ref _hits);
    /// <summary>Reads that went to Redis.</summary>
    public long Misses => Volatile.Read(ref _misses);
    /// <summary>Per-key invalidation messages received from the server.</summary>
    public long Invalidations => Volatile.Read(ref _invalidations);
    /// <summary>Whole-cache flushes (null invalidation, reconnect, re-arm).</summary>
    public long Flushes => Volatile.Read(ref _flushes);
    /// <summary>Times CLIENT TRACKING was (re)issued on an endpoint after the initial arm.</summary>
    public long Rearms => Volatile.Read(ref _rearms);
    /// <summary>Redis replies discarded because an invalidation for the key arrived while the read was in flight.</summary>
    public long RaceDiscards => Volatile.Read(ref _raceDiscards);

    internal void Hit() => Interlocked.Increment(ref _hits);
    internal void Miss() => Interlocked.Increment(ref _misses);
    internal void Invalidation() => Interlocked.Increment(ref _invalidations);
    internal void Flush() => Interlocked.Increment(ref _flushes);
    internal void Rearm() => Interlocked.Increment(ref _rearms);
    internal void RaceDiscard() => Interlocked.Increment(ref _raceDiscards);

    public override string ToString() =>
        $"hits={Hits} misses={Misses} invalidations={Invalidations} flushes={Flushes} rearms={Rearms} raceDiscards={RaceDiscards}";
}
