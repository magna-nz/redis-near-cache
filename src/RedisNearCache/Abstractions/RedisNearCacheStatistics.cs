using RedisNearCache.Internal;

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

    /// <summary>
    /// The <see cref="System.Diagnostics.ActivitySource"/> name RedisNearCache starts its spans under. Pass it to
    /// OpenTelemetry's <c>AddSource</c> (or any other <see cref="System.Diagnostics.ActivityListener"/>) to collect
    /// them. There are two: <c>redisnearcache.read</c> around the Redis round trip of a read that missed the local
    /// cache (a local hit starts no span at all), and <c>redisnearcache.arm</c> around arming <c>CLIENT TRACKING</c>
    /// on one endpoint. Both carry the same <c>rnc.client_name</c> tag the measurements carry.
    /// <para>
    /// Deliberately the same string as <see cref="MeterName"/>: the metrics and the spans of one cache instance are
    /// one instrumentation scope, so a caller who has registered the meter name knows this one too.
    /// </para>
    /// </summary>
    public const string ActivitySourceName = MeterName;

    private long _hits, _misses, _invalidations, _flushes, _rearms, _raceDiscards, _serializerFailures;

    /// <summary>
    /// Per-reason breakdown of <see cref="Rearms"/> and <see cref="Flushes"/>, indexed by the enum value. An array
    /// rather than a dictionary because both are written from event handlers: an indexed
    /// <see cref="Interlocked.Increment(ref long)"/> allocates nothing and needs no lock. Deliberately not public -
    /// the breakdown is a metric tag (<c>reason</c>), not API surface.
    /// </summary>
    private readonly long[] _rearmsByReason = new long[ArmReasonCount];
    private readonly long[] _flushesByReason = new long[FlushReasonCount];

    private static readonly int ArmReasonCount = Enum.GetValues<ArmReason>().Length;
    private static readonly int FlushReasonCount = Enum.GetValues<FlushReason>().Length;

    /// <summary>
    /// Reads the current L1 entry count. Null until the owning cache attaches itself, and again once it is
    /// disposed, because this object outlives the L1 store it reports on but does not own it.
    /// </summary>
    private volatile Func<long>? _l1Count;

    // The same arrangement for everything the owning cache computes rather than counts: state that already exists on
    // the facade, on its L1 store or on its armer, read only when something collects. Null until the cache attaches
    // itself and again once it is disposed, so a dead cache reports 0/false instead of a value that keeps moving.
    private volatile Func<double>? _passThroughSeconds;
    private volatile Func<long>? _lostEndpointCount;
    private volatile Func<bool>? _ttlCapAbandoned;
    private volatile Func<bool>? _untrackedReadsUnavailable;
    private volatile Func<long>? _l1StoreRefusals;
    private volatile Func<long>? _preArmFailures;

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
    /// Times the configured <see cref="IRedisNearCacheSerializer"/> threw. The exception itself is never swallowed -
    /// it reaches the caller unchanged - so this is only here to make a serializer that fails for one type, or for
    /// one value in a hundred, visible in a dashboard rather than only in whatever the caller does with the throw.
    /// </summary>
    public long SerializerFailures => Volatile.Read(ref _serializerFailures);

    /// <summary>
    /// Entries currently held in L1. Unlike the other members this is a live gauge, not a counter: it rises with
    /// every stored read and falls with every invalidation, expiry or size-limit eviction. Zero before the cache
    /// is running and after it has been disposed. Counting asks the L1 store, which briefly takes its locks: fine
    /// for a metrics scrape, a health check or a periodic log line, not something to read on every request (the
    /// same goes for <see cref="ToString"/>, which includes it).
    /// </summary>
    public long L1Entries => Read(_l1Count);

    /// <summary>
    /// How long the cache has been in pass-through (<see cref="IRedisNearCache.IsCoherent"/> false), in seconds;
    /// 0 while it is coherent. Measured from a monotonic clock, so it is unaffected by a system clock change.
    /// <para>
    /// A brand-new cache is in pass-through until its first arm, so this reports a growing duration from
    /// construction rather than 0: an instance that never manages to arm is exactly the case worth alerting on,
    /// and it looks identical to a healthy one in every counter above (no flushes, no re-arms, no invalidations).
    /// </para>
    /// Zero on a disposed cache, which has stopped being anything.
    /// </summary>
    public double PassThroughSeconds
    {
        get
        {
            var read = _passThroughSeconds;
            if (read is null) return 0;
            try
            {
                return read();
            }
            catch (ObjectDisposedException)
            {
                // A dispose racing this read: see L1Entries.
                return 0;
            }
        }
    }

    /// <summary>
    /// Endpoints whose tracking is currently lost, i.e. masters the cache is waiting on before it will serve L1
    /// again. Non-zero means pass-through; zero does not mean coherent on its own (a cache whose start failed, or
    /// has not finished, is in pass-through with an empty lost set). The addresses are deliberately not exposed
    /// here or in any metric tag - they are unbounded cardinality - only in the health check's <c>Data</c> and logs.
    /// </summary>
    public long LostEndpointCount => Read(_lostEndpointCount);

    /// <summary>
    /// True once the server has refused to answer <c>PTTL</c> often enough that
    /// <see cref="RedisNearCacheOptions.RespectServerTtl"/> has been given up on: L1 entries are capped by
    /// <see cref="RedisNearCacheOptions.L1MaxAge"/> alone from then on, as with the option off. Latched, never reset.
    /// It is logged once when it happens; this is how to see it afterwards.
    /// </summary>
    public bool TtlCapAbandoned => Read(_ttlCapAbandoned);

    /// <summary>
    /// True once the server has refused <c>CLIENT CACHING NO</c> inside a transaction: reads of keys outside
    /// <see cref="RedisNearCacheOptions.KeyPrefixes"/> are plain, tracked <c>GET</c>s from then on, so the server
    /// keeps a tracking table entry for keys this cache will never store. Latched, never reset.
    /// </summary>
    public bool UntrackedReadsUnavailable => Read(_untrackedReadsUnavailable);

    /// <summary>
    /// Stores L1 refused for a reason other than the value being bigger than the whole byte budget - the symptom of
    /// MemoryCache's size accounting drifting (see <c>L1Cache</c>'s remarks). Steadily non-zero means this instance
    /// is reading through to Redis while every counter above still looks healthy.
    /// </summary>
    public long L1StoreRefusals => Read(_l1StoreRefusals);

    /// <summary>
    /// Failed attempts to pre-arm a replica. A pre-arm that never succeeds is silent: nothing is wrong until that
    /// replica is promoted, at which point the failover costs a re-arm and a flush that a pre-arm would have saved.
    /// Always 0 in <see cref="TrackingMode.Broadcast"/>, which does not pre-arm replicas.
    /// </summary>
    public long PreArmFailures => Read(_preArmFailures);

    /// <summary>Points <see cref="L1Entries"/> at the owning cache's L1 store; <see cref="DetachL1"/> undoes it on dispose.</summary>
    internal void AttachL1(Func<long> count) => _l1Count = count;

    /// <summary>Called before the L1 store is disposed, so <see cref="L1Entries"/> never reaches a disposed cache.</summary>
    internal void DetachL1() => _l1Count = null;

    /// <summary>
    /// Points the computed members at the owning cache's own state, the same way <see cref="AttachL1"/> does for
    /// <see cref="L1Entries"/>; <see cref="DetachFacade"/> undoes it on dispose. Every reader here must be cheap and
    /// lock-free enough for a collection callback - they are read when something collects, never on the read path.
    /// </summary>
    internal void AttachFacade(
        Func<double> passThroughSeconds,
        Func<long> lostEndpointCount,
        Func<bool> ttlCapAbandoned,
        Func<bool> untrackedReadsUnavailable,
        Func<long> l1StoreRefusals,
        Func<long> preArmFailures)
    {
        _passThroughSeconds = passThroughSeconds;
        _lostEndpointCount = lostEndpointCount;
        _ttlCapAbandoned = ttlCapAbandoned;
        _untrackedReadsUnavailable = untrackedReadsUnavailable;
        _l1StoreRefusals = l1StoreRefusals;
        _preArmFailures = preArmFailures;
    }

    /// <summary>
    /// Called on dispose, with <see cref="DetachL1"/>: a disposed cache reports 0/false for everything computed from
    /// its state rather than a <see cref="PassThroughSeconds"/> that would grow for the life of the process.
    /// </summary>
    internal void DetachFacade()
    {
        _passThroughSeconds = null;
        _lostEndpointCount = null;
        _ttlCapAbandoned = null;
        _untrackedReadsUnavailable = null;
        _l1StoreRefusals = null;
        _preArmFailures = null;
    }

    /// <summary>The <see cref="L1Entries"/> guard, for every other attached reader: unattached or disposed reads as default.</summary>
    private static T Read<T>(Func<T>? read) where T : struct
    {
        if (read is null) return default;
        try
        {
            return read();
        }
        catch (ObjectDisposedException)
        {
            return default;
        }
    }

    internal void Hit() => Interlocked.Increment(ref _hits);
    internal void Miss() => Interlocked.Increment(ref _misses);
    internal void Invalidation() => Interlocked.Increment(ref _invalidations);
    internal void RaceDiscard() => Interlocked.Increment(ref _raceDiscards);
    internal void SerializerFailure() => Interlocked.Increment(ref _serializerFailures);

    /// <summary>
    /// One whole-cache flush. <see cref="Flushes"/> counts every one of them exactly as before; the reason is only
    /// the extra per-reason slot, so the total and the sum over the reasons always agree.
    /// </summary>
    internal void Flush(FlushReason reason)
    {
        Interlocked.Increment(ref _flushes);
        Count(_flushesByReason, (int)reason);
    }

    /// <summary>
    /// One re-arm. Called for every <see cref="ArmReason"/> except <see cref="ArmReason.Initial"/> and
    /// <see cref="ArmReason.Promoted"/>, which are not re-arms; see the caller.
    /// </summary>
    internal void Rearm(ArmReason reason)
    {
        Interlocked.Increment(ref _rearms);
        Count(_rearmsByReason, (int)reason);
    }

    /// <summary>The per-reason slot of one re-arm reason; the sum over every reason equals <see cref="Rearms"/>.</summary>
    internal long RearmsFor(ArmReason reason) => Slot(_rearmsByReason, (int)reason);

    /// <summary>The per-reason slot of one flush reason; the sum over every reason equals <see cref="Flushes"/>.</summary>
    internal long FlushesFor(FlushReason reason) => Slot(_flushesByReason, (int)reason);

    // Bounds-checked rather than trusting the cast: a value outside the enum would otherwise throw from an event
    // handler, where nothing would catch it, and losing one breakdown slot is not worth that.
    private static void Count(long[] slots, int index)
    {
        if ((uint)index < (uint)slots.Length) Interlocked.Increment(ref slots[index]);
    }

    private static long Slot(long[] slots, int index) =>
        (uint)index < (uint)slots.Length ? Volatile.Read(ref slots[index]) : 0;

    /// <summary>
    /// One-line summary of all counters. New fields are appended, so the existing prefix stays stable. The one
    /// non-integer field is formatted invariantly: this goes in log lines, which must not read differently per host.
    /// </summary>
    public override string ToString() =>
        $"hits={Hits} misses={Misses} invalidations={Invalidations} flushes={Flushes} rearms={Rearms} raceDiscards={RaceDiscards} l1Entries={L1Entries}" +
        $" passThroughSeconds={PassThroughSeconds.ToString("F1", System.Globalization.CultureInfo.InvariantCulture)} lostEndpointCount={LostEndpointCount} ttlCapAbandoned={TtlCapAbandoned}" +
        $" untrackedReadsUnavailable={UntrackedReadsUnavailable} serializerFailures={SerializerFailures}" +
        $" l1StoreRefusals={L1StoreRefusals} preArmFailures={PreArmFailures}";
}
