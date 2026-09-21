using System.Diagnostics.Metrics;
using System.Reflection;
using RedisNearCache.Internal;

namespace RedisNearCache.Caching;

/// <summary>
/// Publishes <see cref="RedisNearCacheStatistics"/> on a <see cref="Meter"/> named
/// <see cref="RedisNearCacheStatistics.MeterName"/>, one meter per cache instance.
/// </summary>
/// <remarks>
/// Every instrument is OBSERVABLE: nothing is recorded on the read path, the collector pulls the counters that
/// are kept anyway. That is what makes metrics free when nobody is listening, and it is why adding an instrument
/// here must never mean adding a write to <see cref="RedisNearCache"/>'s hot path.
/// <para>
/// Each measurement carries the Redis client name of this instance's private connection, so a process that builds
/// more than one cache (different prefixes, different databases) can tell their series apart.
/// </para>
/// </remarks>
internal sealed class RedisNearCacheMetrics : IDisposable
{
    /// <summary>The meter name; the same constant the public <see cref="RedisNearCacheStatistics"/> exposes to callers.</summary>
    public const string MeterName = RedisNearCacheStatistics.MeterName;

    /// <summary>The tag every measurement carries, naming the cache instance by its Redis client name.</summary>
    public const string ClientNameTag = "rnc.client_name";

    /// <summary>
    /// A second tag, carried only by an instance registered under a name (<c>AddKeyedRedisNearCache</c>): the client
    /// name changes with every process, the registration name does not. The default instance's series are unchanged.
    /// </summary>
    public const string InstanceTag = "rnc.instance";

    /// <summary>
    /// The tag that carries the per-reason breakdown of <c>redisnearcache.rearms</c> and
    /// <c>redisnearcache.flushes</c>. Its values are <see cref="ArmReason"/> / <see cref="FlushReason"/> member
    /// names, a closed set, so the cardinality it adds is fixed and small - unlike an endpoint address, which is why
    /// no instrument here is tagged with one.
    /// </summary>
    public const string ReasonTag = "reason";

    /// <summary>
    /// The re-arm reasons: every <see cref="ArmReason"/> except <see cref="ArmReason.Initial"/> and
    /// <see cref="ArmReason.Promoted"/>. Those two are arms but never re-arms - the facade does not count them
    /// towards <see cref="RedisNearCacheStatistics.Rearms"/> - so emitting them as a permanent 0 would suggest a
    /// re-arm reason that simply never fires, which is not the same thing. The sum over this set is the total.
    /// </summary>
    private static readonly ArmReason[] RearmReasons =
    [
        ArmReason.InteractiveRestored,
        ArmReason.SubscriptionRestored,
        ArmReason.Manual,
        ArmReason.TopologyChanged,
        ArmReason.Recovered,
        ArmReason.VerificationFailed,
        ArmReason.PushConnectionRestored,
    ];

    private static readonly FlushReason[] FlushReasons = Enum.GetValues<FlushReason>();

    // Resolved once: Enum.GetName allocates, and these are read on every collection cycle.
    private static readonly string[] RearmReasonNames = Array.ConvertAll(RearmReasons, r => Enum.GetName(r) ?? r.ToString());
    private static readonly string[] FlushReasonNames = Array.ConvertAll(FlushReasons, r => Enum.GetName(r) ?? r.ToString());

    /// <summary>
    /// The package version, as the version of both instrumentation scopes: this <see cref="Meter"/> and
    /// <see cref="RedisNearCacheTracing"/>'s <see cref="System.Diagnostics.ActivitySource"/>, which share a name and
    /// must therefore share a version. Without the "+&lt;commit sha&gt;" a SourceLink build appends: it would only be
    /// noise in the scope.
    /// </summary>
    internal static readonly string? InstrumentationVersion =
        typeof(RedisNearCacheMetrics).Assembly.GetCustomAttribute<AssemblyInformationalVersionAttribute>()?.InformationalVersion.Split('+')[0];

    private readonly Meter _meter;
    private readonly KeyValuePair<string, object?>[] _tags;

    // The instance tags plus one reason tag, built once per reason rather than per collection cycle.
    private readonly KeyValuePair<string, object?>[][] _rearmReasonTags;
    private readonly KeyValuePair<string, object?>[][] _flushReasonTags;

    public RedisNearCacheMetrics(RedisNearCacheStatistics statistics, string clientName, Func<bool> isCoherent, string? instanceName = null)
    {
        _tags = instanceName is null
            ? [new KeyValuePair<string, object?>(ClientNameTag, clientName)]
            : [new KeyValuePair<string, object?>(ClientNameTag, clientName), new KeyValuePair<string, object?>(InstanceTag, instanceName)];
        _rearmReasonTags = Array.ConvertAll(RearmReasonNames, TagsWithReason);
        _flushReasonTags = Array.ConvertAll(FlushReasonNames, TagsWithReason);
        _meter = new Meter(MeterName, InstrumentationVersion);

        _meter.CreateObservableCounter("redisnearcache.hits", () => Observe(() => statistics.Hits),
            "{read}", "Reads served from L1 without touching Redis.");
        _meter.CreateObservableCounter("redisnearcache.misses", () => Observe(() => statistics.Misses),
            "{read}", "Reads that went to Redis.");
        _meter.CreateObservableCounter("redisnearcache.invalidations", () => Observe(() => statistics.Invalidations),
            "{message}", "Per-key invalidation messages received from the server.");
        // Broken out by reason: the total is still the sum over the reason tag, so a dashboard that ignores the tag
        // reads exactly what it read before.
        _meter.CreateObservableCounter("redisnearcache.flushes", () => ObserveByReason(FlushReasons, _flushReasonTags, statistics.FlushesFor),
            "{flush}", "Whole-cache flushes, by reason (null invalidation, reconnect, re-arm, endpoint removal, manual).");
        _meter.CreateObservableCounter("redisnearcache.rearms", () => ObserveByReason(RearmReasons, _rearmReasonTags, statistics.RearmsFor),
            "{arm}", "Times CLIENT TRACKING was re-issued on an endpoint after the initial arm, by reason.");
        _meter.CreateObservableCounter("redisnearcache.race_discards", () => Observe(() => statistics.RaceDiscards),
            "{read}", "Redis replies discarded because an invalidation arrived while the read was in flight.");
        _meter.CreateObservableCounter("redisnearcache.serializer_failures", () => Observe(() => statistics.SerializerFailures),
            "{failure}", "Times the configured serializer threw. The exception still reaches the caller unchanged.");
        _meter.CreateObservableCounter("redisnearcache.l1.store_refusals", () => Observe(() => statistics.L1StoreRefusals),
            "{store}", "Stores L1 refused for a reason other than the value exceeding the whole byte budget.");
        _meter.CreateObservableCounter("redisnearcache.prearm_failures", () => Observe(() => statistics.PreArmFailures),
            "{failure}", "Failed attempts to pre-arm a replica, each one a failover that will cost a re-arm and a flush.");

        _meter.CreateObservableGauge("redisnearcache.l1.entries", () => Observe(() => statistics.L1Entries),
            "{entry}", "Entries currently held in L1.");
        _meter.CreateObservableGauge("redisnearcache.coherent", () => Observe(() => isCoherent() ? 1L : 0L),
            "{state}", "1 while tracking is armed everywhere and L1 is being read and populated, otherwise 0.");
        _meter.CreateObservableGauge("redisnearcache.pass_through.seconds", () => ObserveSeconds(() => statistics.PassThroughSeconds),
            "s", "Seconds the cache has been in pass-through; 0 while coherent. Counts from construction until the first arm.");
        _meter.CreateObservableGauge("redisnearcache.endpoints.lost", () => Observe(() => statistics.LostEndpointCount),
            "{endpoint}", "Masters whose tracking is lost and which the cache is waiting on before serving L1 again.");
        _meter.CreateObservableGauge("redisnearcache.ttl_cap.abandoned", () => Observe(() => statistics.TtlCapAbandoned ? 1L : 0L),
            "{state}", "1 once RespectServerTtl has been given up on because the server would not answer PTTL, otherwise 0.");
        _meter.CreateObservableGauge("redisnearcache.untracked_reads.unavailable", () => Observe(() => statistics.UntrackedReadsUnavailable ? 1L : 0L),
            "{state}", "1 once the server refused CLIENT CACHING NO in a transaction, so reads outside KeyPrefixes are tracked, otherwise 0.");
    }

    private KeyValuePair<string, object?>[] TagsWithReason(string reason)
    {
        var tags = new KeyValuePair<string, object?>[_tags.Length + 1];
        _tags.CopyTo(tags, 0);
        tags[^1] = new KeyValuePair<string, object?>(ReasonTag, reason);
        return tags;
    }

    /// <summary>
    /// A collection callback that throws takes the whole collection cycle down, so every reading is guarded: a cache
    /// disposed mid-collection reports nothing held rather than failing the collector. The counters themselves
    /// outlive the cache and keep reporting their final totals.
    /// </summary>
    private Measurement<long> Observe(Func<long> read)
    {
        try
        {
            return new Measurement<long>(read(), _tags);
        }
        catch
        {
            return new Measurement<long>(0, _tags);
        }
    }

    /// <summary>
    /// The <see cref="Observe(Func{long})"/> sibling for the one instrument that is not a whole number of things.
    /// Same tags, same guard; a separate method because <see cref="Measurement{T}"/> is generic in the value type and
    /// the existing instruments are all <see cref="long"/>.
    /// </summary>
    private Measurement<double> ObserveSeconds(Func<double> read)
    {
        try
        {
            return new Measurement<double>(read(), _tags);
        }
        catch
        {
            return new Measurement<double>(0, _tags);
        }
    }

    /// <summary>
    /// One measurement per reason, every collection cycle, zeros included: an instrument that disappears when its
    /// count is 0 shows up as no-data on a dashboard, which reads like a broken exporter rather than like nothing
    /// having gone wrong. Guarded per reason, for the reason on <see cref="Observe(Func{long})"/>.
    /// </summary>
    private static Measurement<long>[] ObserveByReason<TReason>(TReason[] reasons, KeyValuePair<string, object?>[][] tags, Func<TReason, long> read)
    {
        var measurements = new Measurement<long>[reasons.Length];
        for (var i = 0; i < reasons.Length; i++)
        {
            long value;
            try
            {
                value = read(reasons[i]);
            }
            catch
            {
                value = 0;
            }

            measurements[i] = new Measurement<long>(value, tags[i]);
        }

        return measurements;
    }

    public void Dispose() => _meter.Dispose();
}
