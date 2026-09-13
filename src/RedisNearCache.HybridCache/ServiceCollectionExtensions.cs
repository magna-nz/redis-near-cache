using Microsoft.Extensions.Caching.Distributed;
using Microsoft.Extensions.Caching.Hybrid;
using Microsoft.Extensions.DependencyInjection;

namespace RedisNearCache.HybridCache;

/// <summary>
/// Registers <see cref="RedisNearCacheDistributedCache"/> as the standard distributed-cache abstractions,
/// and optionally wires it up as the backing store for <c>Microsoft.Extensions.Caching.Hybrid.HybridCache</c>.
/// </summary>
public static class ServiceCollectionExtensions
{
    /// <summary>
    /// The <see cref="HybridCacheEntryOptions.LocalCacheExpiration"/> used to make <c>HybridCache</c>'s own L1
    /// a practical no-op. See <see cref="AddRedisNearCacheHybridCache(IServiceCollection, Action{HybridCacheOptions}?)"/>
    /// for why this is ten milliseconds rather than <see cref="TimeSpan.Zero"/> or the true smallest positive
    /// <see cref="TimeSpan"/> (one tick).
    /// </summary>
    private static readonly TimeSpan DisabledLocalCacheExpiration = TimeSpan.FromMilliseconds(10);

    /// <summary>
    /// Registers <see cref="RedisNearCacheDistributedCache"/> as a singleton, exposed as both
    /// <see cref="IDistributedCache"/> and <see cref="IBufferDistributedCache"/>, backed by the
    /// <see cref="IRedisNearCache"/> that <c>AddRedisNearCache</c> must already have registered. Resolving
    /// either interface without a prior call to <c>AddRedisNearCache</c> throws
    /// <see cref="InvalidOperationException"/> (from resolving <see cref="IRedisNearCache"/>), not from this
    /// method itself.
    /// </summary>
    public static IServiceCollection AddRedisNearCacheDistributedCache(this IServiceCollection services)
    {
        ArgumentNullException.ThrowIfNull(services);

        services.AddSingleton(sp => new RedisNearCacheDistributedCache(sp.GetRequiredService<IRedisNearCache>()));
        services.AddSingleton<IDistributedCache>(sp => sp.GetRequiredService<RedisNearCacheDistributedCache>());
        services.AddSingleton<IBufferDistributedCache>(sp => sp.GetRequiredService<RedisNearCacheDistributedCache>());

        return services;
    }

    /// <summary>
    /// Registers <see cref="RedisNearCacheDistributedCache"/> (via
    /// <see cref="AddRedisNearCacheDistributedCache"/>) and then calls <c>AddHybridCache</c>, so that
    /// <c>Microsoft.Extensions.Caching.Hybrid.HybridCache</c> uses RedisNearCache as its distributed tier.
    /// Requires a prior call to <c>AddRedisNearCache</c>.
    /// </summary>
    /// <remarks>
    /// Unless <paramref name="configure"/> overrides it, this sets
    /// <see cref="HybridCacheOptions.DefaultEntryOptions"/>.<see cref="HybridCacheEntryOptions.LocalCacheExpiration"/>
    /// to ten milliseconds (<see cref="DisabledLocalCacheExpiration"/>), which is as close to "disabled" as
    /// <c>HybridCache</c> can be pushed in practice:
    /// <list type="bullet">
    /// <item><description>
    /// <see cref="TimeSpan.Zero"/> cannot be used: <c>HybridCache</c>'s underlying <c>MemoryCache</c> throws
    /// <see cref="ArgumentOutOfRangeException"/> for a non-positive relative expiration.
    /// </description></item>
    /// <item><description>
    /// The true smallest positive <see cref="TimeSpan"/> (one tick, <c>new TimeSpan(1)</c>) is technically
    /// accepted but is not reliable: <c>HybridCache</c> persists a newly-computed value to the distributed
    /// cache (this adapter) in the background rather than awaiting it before returning, so a second
    /// <c>GetOrCreateAsync</c> call for the same key immediately afterwards can race the still-in-flight
    /// write, see it as neither an L1 hit (already expired) nor an L2 hit (not yet written), and re-run the
    /// factory even though nothing invalidated the entry. Empirically (against a local Redis) a one-tick or
    /// one-millisecond window is not enough margin for that background write to reliably land first; ten
    /// milliseconds was, across repeated runs. This still bounds how long a value can survive in
    /// <c>HybridCache</c>'s own untracked L1 to something small relative to <see cref="RedisNearCacheOptions.L1MaxAge"/>'s
    /// default (five minutes).
    /// </description></item>
    /// </list>
    ///
    /// <para>
    /// <c>HybridCache</c> keeps its own in-process L1 in front of whatever <see cref="IDistributedCache"/> it is
    /// given, and that L1 knows nothing about Redis <c>CLIENT TRACKING</c> invalidations: only RedisNearCache's
    /// own L1 (inside <see cref="RedisNearCacheDistributedCache"/>) is evicted when the server invalidates a
    /// key. If <c>HybridCache</c>'s L1 were allowed to hold entries for any meaningful duration, it could keep
    /// serving a value that RedisNearCache's L1 has already evicted — two L1s, only one of which is coherent.
    /// Capping <see cref="HybridCacheEntryOptions.LocalCacheExpiration"/> at ten milliseconds means almost
    /// every request falls through to <see cref="IDistributedCache"/> (this adapter), whose L1 is the tracked,
    /// coherent one; the residual ten-millisecond window is a known, deliberately small gap needed purely to
    /// mask <c>HybridCache</c>'s background L2 persistence, not a design goal in itself.
    /// </para>
    ///
    /// <para>
    /// <see cref="HybridCacheEntryOptions.Expiration"/> (the overall/distributed lifetime) is left at
    /// <c>HybridCache</c>'s own default unless <paramref name="configure"/> sets it.
    /// </para>
    /// </remarks>
    public static IServiceCollection AddRedisNearCacheHybridCache(
        this IServiceCollection services,
        Action<HybridCacheOptions>? configure = null)
    {
        ArgumentNullException.ThrowIfNull(services);

        services.AddRedisNearCacheDistributedCache();
        services.AddHybridCache(options =>
        {
            // HybridCacheEntryOptions is effectively immutable (init-only properties), so disabling
            // HybridCache's own L1 means creating the DefaultEntryOptions object here rather than mutating
            // one. Expiration and Flags are left unset (null), i.e. at HybridCache's own defaults.
            options.DefaultEntryOptions = new HybridCacheEntryOptions { LocalCacheExpiration = DisabledLocalCacheExpiration };
            configure?.Invoke(options);
        });

        return services;
    }
}
