using Microsoft.Extensions.Diagnostics.HealthChecks;

namespace RedisNearCache;

/// <summary>
/// Reports <see cref="IRedisNearCache.IsCoherent"/> through <c>Microsoft.Extensions.Diagnostics.HealthChecks</c>.
/// </summary>
/// <remarks>
/// <para>
/// Register it explicitly with a name of your choosing; RedisNearCache does not add an <c>IHealthChecksBuilder</c>
/// extension because there is nothing to configure beyond resolving <see cref="IRedisNearCache"/>:
/// </para>
/// <code>
/// services.AddHealthChecks().AddCheck&lt;RedisNearCacheHealthCheck&gt;("redis-near-cache");
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
    /// safe to call even on a disposed cache (where <c>IsCoherent</c> is false and every counter reads its last
    /// value, reporting Degraded).
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
        };

        var result = coherent
            ? HealthCheckResult.Healthy("RedisNearCache is coherent: tracking is armed on every master and reads are served from L1.", data)
            // Not FailureStatus: reads still succeed as pass-through to Redis while tracking is being (re)armed,
            // so this is never Unhealthy - see the type-level remarks.
            : HealthCheckResult.Degraded("RedisNearCache is in pass-through: tracking is being (re)armed, so reads go straight to Redis and nothing is served from or stored in L1.", data: data);

        return Task.FromResult(result);
    }
}
