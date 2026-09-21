using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;

namespace RedisNearCache;

/// <summary>
/// Reports <see cref="IRedisNearCache.IsCoherent"/> through <c>Microsoft.Extensions.Diagnostics.HealthChecks</c>.
/// </summary>
/// <remarks>
/// <para>
/// RedisNearCache does not add an <c>IHealthChecksBuilder</c> extension because <c>IHealthChecksBuilder</c> lives
/// in <c>Microsoft.Extensions.Diagnostics.HealthChecks</c>, a package this library deliberately does not
/// reference (it depends only on the Abstractions package, which exports <see cref="IHealthCheck"/> and
/// <see cref="HealthCheckRegistration"/> but not the builder). Use <see cref="Registration"/> instead to get a
/// ready-made registration for the builder you already have, including for a cache registered with a service key
/// (<c>AddKeyedRedisNearCache</c>), which plain <c>AddCheck&lt;RedisNearCacheHealthCheck&gt;</c> cannot resolve:
/// </para>
/// <code>
/// services.AddHealthChecks().Add(RedisNearCacheHealthCheck.Registration(serviceKey: "orders"));
/// </code>
/// <para>
/// While <see cref="IRedisNearCache.IsCoherent"/> is false the cache is in pass-through: every read still
/// succeeds by going straight to Redis, nothing is served from or stored in L1. That is degraded service, not
/// an outage, so this check reports <see cref="HealthStatus.Degraded"/> rather than
/// <see cref="HealthStatus.Unhealthy"/> and ignores <see cref="HealthCheckRegistration.FailureStatus"/> for that
/// case - an orchestrator that treats Unhealthy as "take this instance out of rotation" would otherwise pull a
/// perfectly serviceable instance out of the pool while it is (re)arming tracking.
/// </para>
/// </remarks>
public sealed class RedisNearCacheHealthCheck : IHealthCheck
{
    /// <summary>The name a registration produced by <see cref="Registration"/> uses for the default (non-keyed) instance.</summary>
    private const string DefaultName = "redis-near-cache";

    /// <summary>
    /// Builds a <see cref="HealthCheckRegistration"/> for an <see cref="IRedisNearCache"/> already registered with
    /// <c>services</c>, default or keyed, to hand to the <c>IHealthChecksBuilder</c> you already have (see the
    /// type-level remarks for why this library does not add its own builder extension).
    /// </summary>
    /// <param name="name">
    /// The registration name. Defaults to <c>"redis-near-cache"</c> for the default instance, or
    /// <c>$"redis-near-cache-{serviceKey}"</c> for a keyed one, so two keyed registrations cannot collide.
    /// </param>
    /// <param name="serviceKey">
    /// <see langword="null"/> to resolve the default <see cref="IRedisNearCache"/>; otherwise the key it was
    /// registered under (e.g. with <c>AddKeyedRedisNearCache</c>), resolved with
    /// <see cref="ServiceProviderKeyedServiceExtensions.GetRequiredKeyedService{T}(IServiceProvider, object?)"/>.
    /// </param>
    /// <param name="tags">Optional tags passed through to the registration.</param>
    public static HealthCheckRegistration Registration(string? name = null, string? serviceKey = null, IEnumerable<string>? tags = null)
    {
        var registrationName = name ?? (serviceKey is null ? DefaultName : $"{DefaultName}-{serviceKey}");
        return new HealthCheckRegistration(
            registrationName,
            provider => new RedisNearCacheHealthCheck(serviceKey is null
                ? provider.GetRequiredService<IRedisNearCache>()
                : provider.GetRequiredKeyedService<IRedisNearCache>(serviceKey)),
            // Never Unhealthy by design; see the type-level remarks.
            failureStatus: null,
            tags);
    }

    private readonly IRedisNearCache _cache;

    /// <summary>Creates the health check over an already-registered <see cref="IRedisNearCache"/>.</summary>
    /// <param name="cache">The cache instance to report on.</param>
    public RedisNearCacheHealthCheck(IRedisNearCache cache)
    {
        ArgumentNullException.ThrowIfNull(cache);
        _cache = cache;
    }

    /// <summary>
    /// Reports <see cref="HealthStatus.Healthy"/> while tracking is armed on every master and L1 is being served,
    /// or <see cref="HealthStatus.Degraded"/> while the cache is in pass-through (reads still succeed against
    /// Redis directly). Never throws, never awaits Redis, and never inspects <see cref="IRedisNearCache.Ready"/>:
    /// it only reads the already-computed <see cref="IRedisNearCache.IsCoherent"/> flag and counters, so it is
    /// safe to call even on a disposed cache, which reports Degraded.
    /// <para>
    /// A DISPOSED cache's <c>Data</c> is not simply its last reading, though. The plain counters keep their values
    /// (<c>hits</c>, <c>misses</c>, <c>invalidations</c>, <c>flushes</c>, <c>rearms</c>, <c>raceDiscards</c>,
    /// <c>serializerFailures</c>), because they are fields on <see cref="RedisNearCacheStatistics"/> that nothing
    /// resets, and so does <c>lostEndpoints</c>, which the facade answers from its own set. The seven entries computed
    /// by reading the cache's live state through <c>RedisNearCacheStatistics.AttachL1</c>/<c>AttachFacade</c> read
    /// 0/false instead, because dispose detaches those readers rather than let them reach a disposed store or let
    /// <c>passThroughSeconds</c> grow for the life of the process: <c>l1Entries</c>, <c>passThroughSeconds</c>,
    /// <c>lostEndpointCount</c>, <c>ttlCapAbandoned</c>, <c>untrackedReadsUnavailable</c>, <c>l1StoreRefusals</c> and
    /// <c>preArmFailures</c>. So a disposed cache reads as Degraded with nothing in those entries explaining why, and
    /// <c>lostEndpointCount</c> can be 0 next to a non-empty <c>lostEndpoints</c>. That combination - Degraded,
    /// zeroed - means "disposed", not "healthy but degraded", and it is why a live instance's numbers should be read
    /// from a scrape taken before shutdown, not from the last one.
    /// </para>
    /// </summary>
    /// <param name="context">The health check context. <see cref="HealthCheckRegistration.FailureStatus"/> is
    /// deliberately not consulted; see the type-level remarks.</param>
    /// <param name="cancellationToken">
    /// Unused: this check does no I/O, so there is nothing to cancel. It is honoured only in the sense that this
    /// method never does anything else - it returns an already-completed <see cref="Task{TResult}"/> promptly,
    /// regardless of whether cancellation was requested, and never throws <see cref="OperationCanceledException"/>.
    /// </param>
    public Task<HealthCheckResult> CheckHealthAsync(HealthCheckContext context, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(context);

        // Read once, so the status and the "coherent" entry in Data can never disagree.
        var coherent = _cache.IsCoherent;
        var stats = _cache.Statistics;
        var data = new Dictionary<string, object>
        {
            ["coherent"] = coherent,
            ["hits"] = stats.Hits,
            ["misses"] = stats.Misses,
            ["invalidations"] = stats.Invalidations,
            ["flushes"] = stats.Flushes,
            ["rearms"] = stats.Rearms,
            ["raceDiscards"] = stats.RaceDiscards,
            ["l1Entries"] = stats.L1Entries,
            // Everything below reads Statistics, never the concrete cache, so a caller's own IRedisNearCache
            // implementation reports the same keys (its own counters' defaults) rather than falling over.
            ["passThroughSeconds"] = stats.PassThroughSeconds,
            ["lostEndpointCount"] = stats.LostEndpointCount,
            ["ttlCapAbandoned"] = stats.TtlCapAbandoned,
            ["untrackedReadsUnavailable"] = stats.UntrackedReadsUnavailable,
            ["serializerFailures"] = stats.SerializerFailures,
            ["l1StoreRefusals"] = stats.L1StoreRefusals,
            ["preArmFailures"] = stats.PreArmFailures,
        };

        // The one exception: WHICH endpoints are lost is unbounded cardinality, so it is not on Statistics (and not on
        // any metric tag) - only the concrete cache can answer it. Omitted entirely for any other implementation,
        // rather than reported as an empty string that would read as "nothing is lost".
        if (_cache is Caching.RedisNearCache facade)
        {
            data["lostEndpoints"] = facade.LostEndpointAddresses();
        }

        var result = coherent
            ? HealthCheckResult.Healthy("RedisNearCache is coherent: tracking is armed on every master and reads are served from L1.", data)
            // Not FailureStatus: reads still succeed as pass-through to Redis while tracking is being (re)armed,
            // so this is never Unhealthy - see the type-level remarks.
            : HealthCheckResult.Degraded("RedisNearCache is in pass-through: tracking is being (re)armed, so reads go straight to Redis and nothing is served from or stored in L1.", data: data);

        return Task.FromResult(result);
    }
}
