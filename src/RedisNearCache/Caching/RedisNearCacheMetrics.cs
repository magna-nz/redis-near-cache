using System.Diagnostics.Metrics;
using System.Reflection;

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

    // Without the "+<commit sha>" a SourceLink build appends: it would only be noise in the instrumentation scope.
    private static readonly string? MeterVersion =
        typeof(RedisNearCacheMetrics).Assembly.GetCustomAttribute<AssemblyInformationalVersionAttribute>()?.InformationalVersion.Split('+')[0];

    private readonly Meter _meter;
    private readonly KeyValuePair<string, object?>[] _tags;

    public RedisNearCacheMetrics(RedisNearCacheStatistics statistics, string clientName, Func<bool> isCoherent)
    {
        _tags = [new KeyValuePair<string, object?>(ClientNameTag, clientName)];
        _meter = new Meter(MeterName, MeterVersion);

        _meter.CreateObservableCounter("redisnearcache.hits", () => Observe(() => statistics.Hits),
            "{read}", "Reads served from L1 without touching Redis.");
        _meter.CreateObservableCounter("redisnearcache.misses", () => Observe(() => statistics.Misses),
            "{read}", "Reads that went to Redis.");
        _meter.CreateObservableCounter("redisnearcache.invalidations", () => Observe(() => statistics.Invalidations),
            "{message}", "Per-key invalidation messages received from the server.");
        _meter.CreateObservableCounter("redisnearcache.flushes", () => Observe(() => statistics.Flushes),
            "{flush}", "Whole-cache flushes (null invalidation, reconnect, re-arm).");
        _meter.CreateObservableCounter("redisnearcache.rearms", () => Observe(() => statistics.Rearms),
            "{arm}", "Times CLIENT TRACKING was re-issued on an endpoint after the initial arm.");
        _meter.CreateObservableCounter("redisnearcache.race_discards", () => Observe(() => statistics.RaceDiscards),
            "{read}", "Redis replies discarded because an invalidation arrived while the read was in flight.");

        _meter.CreateObservableGauge("redisnearcache.l1.entries", () => Observe(() => statistics.L1Entries),
            "{entry}", "Entries currently held in L1.");
        _meter.CreateObservableGauge("redisnearcache.coherent", () => Observe(() => isCoherent() ? 1L : 0L),
            "{state}", "1 while tracking is armed everywhere and L1 is being read and populated, otherwise 0.");
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

    public void Dispose() => _meter.Dispose();
}
