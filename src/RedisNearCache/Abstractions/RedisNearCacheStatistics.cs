namespace RedisNearCache;

/// <summary>Thread-safe counters exposed by <see cref="IRedisNearCache.Statistics"/>.</summary>
public sealed class RedisNearCacheStatistics
{
    /// <summary>
    /// The <see cref="System.Diagnostics.Metrics.Meter"/> name RedisNearCache publishes its instruments under.
    /// Pass it to OpenTelemetry's <c>AddMeter</c> (or any other <see cref="System.Diagnostics.Metrics.MeterListener"/>)
    /// to collect them. Every measurement carries an <c>rnc.client_name</c> tag identifying the cache instance.
    /// </summary>
    public const string MeterName = "RedisNearCache";

    private long _hits, _misses, _invalidations, _flushes, _rearms, _raceDiscards;

    /// <summary>
    /// Reads the current L1 entry count. Null until the owning cache attaches itself, and again once it is
    /// disposed, because this object outlives the L1 store it reports on but does not own it.
    /// </summary>
    private volatile Func<long>? _l1Count;

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

    /// <summary>
    /// Entries currently held in L1. Unlike the other members this is a live gauge, not a counter: it rises with
    /// every stored read and falls with every invalidation, expiry or size-limit eviction. Zero before the cache
    /// is running and after it has been disposed. Counting asks the L1 store, which briefly takes its locks: fine
    /// for a metrics scrape, a health check or a periodic log line, not something to read on every request (the
    /// same goes for <see cref="ToString"/>, which includes it).
    /// </summary>
    public long L1Entries
    {
        get
        {
            var count = _l1Count;
            if (count is null) return 0;
            try
            {
                return count();
            }
            catch (ObjectDisposedException)
            {
                // A dispose racing this read: the L1 store is gone, so it holds nothing.
                return 0;
            }
        }
    }

    /// <summary>Points <see cref="L1Entries"/> at the owning cache's L1 store; <see cref="DetachL1"/> undoes it on dispose.</summary>
    internal void AttachL1(Func<long> count) => _l1Count = count;

    /// <summary>Called before the L1 store is disposed, so <see cref="L1Entries"/> never reaches a disposed cache.</summary>
    internal void DetachL1() => _l1Count = null;

    internal void Hit() => Interlocked.Increment(ref _hits);
    internal void Miss() => Interlocked.Increment(ref _misses);
    internal void Invalidation() => Interlocked.Increment(ref _invalidations);
    internal void Flush() => Interlocked.Increment(ref _flushes);
    internal void Rearm() => Interlocked.Increment(ref _rearms);
    internal void RaceDiscard() => Interlocked.Increment(ref _raceDiscards);

    /// <summary>One-line summary of all counters. New fields are appended, so the existing prefix stays stable.</summary>
    public override string ToString() =>
        $"hits={Hits} misses={Misses} invalidations={Invalidations} flushes={Flushes} rearms={Rearms} raceDiscards={RaceDiscards} l1Entries={L1Entries}";
}
