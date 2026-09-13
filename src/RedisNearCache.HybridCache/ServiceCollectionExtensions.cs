using Microsoft.Extensions.Caching.Distributed;
using Microsoft.Extensions.Caching.Hybrid;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace RedisNearCache.HybridCache;

/// <summary>
/// Registers <see cref="RedisNearCacheDistributedCache"/> as the standard distributed-cache abstractions,
/// and optionally wires it up as the backing store for <c>Microsoft.Extensions.Caching.Hybrid.HybridCache</c>.
/// </summary>
public static class ServiceCollectionExtensions
{

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

        services.TryAddSingleton(sp => new RedisNearCacheDistributedCache(sp.GetRequiredService<IRedisNearCache>()));
        services.TryAddSingleton<IDistributedCache>(sp => sp.GetRequiredService<RedisNearCacheDistributedCache>());
        services.TryAddSingleton<IBufferDistributedCache>(sp => sp.GetRequiredService<RedisNearCacheDistributedCache>());

        return services;
    }

    /// <summary>
    /// Registers <see cref="RedisNearCacheDistributedCache"/> (via
    /// <see cref="AddRedisNearCacheDistributedCache"/>) and then calls <c>AddHybridCache</c>, so that
    /// <c>Microsoft.Extensions.Caching.Hybrid.HybridCache</c> uses RedisNearCache as its distributed tier.
    /// Requires a prior call to <c>AddRedisNearCache</c>.
    /// </summary>
    /// <remarks>
    /// <para>
    /// <c>HybridCache</c> keeps its own in-process L1 in front of whatever <see cref="IDistributedCache"/> it is
    /// given, and that L1 knows nothing about Redis <c>CLIENT TRACKING</c> invalidations: only RedisNearCache's
    /// own L1 (inside <see cref="RedisNearCacheDistributedCache"/>) is evicted when the server invalidates a
    /// key. Two L1s, only one of them coherent, would let <c>HybridCache</c> keep serving a value RedisNearCache
    /// already evicted. So unless <paramref name="configure"/> overrides it, this sets
    /// <see cref="HybridCacheOptions.DefaultEntryOptions"/>.<see cref="HybridCacheEntryOptions.Flags"/> to
    /// <see cref="HybridCacheEntryFlags.DisableLocalCache"/>: every <c>HybridCache</c> read goes to
    /// <see cref="IDistributedCache"/>, i.e. to RedisNearCache's tracked L1, which is just as fast.
    /// </para>
    /// <para>
    /// Flags are the right lever because <c>HybridCache</c> merges them per call: a
    /// <see cref="HybridCacheEntryOptions"/> passed to <c>GetOrCreateAsync</c> with <c>Flags == null</c>
    /// inherits the default flags. A small <see cref="HybridCacheEntryOptions.LocalCacheExpiration"/> would
    /// not do: a per-call <see cref="HybridCacheEntryOptions.Expiration"/> silently becomes the local
    /// expiration too, reviving the untracked L1 for the whole entry lifetime. Callers who pass explicit
    /// <c>Flags</c> per call take responsibility for keeping <see cref="HybridCacheEntryFlags.DisableLocalCache"/> in them.
    /// </para>
    /// <para>
    /// One consequence: <c>HybridCache</c> persists a freshly computed value to the distributed tier in the
    /// background, so a second <c>GetOrCreateAsync</c> for the same key issued before that write lands (a few
    /// hundred microseconds to a few milliseconds) may run the factory again. Concurrent callers are still
    /// coalesced by <c>HybridCache</c>'s stampede protection. This is an occasional extra factory call, never a stale read.
    /// </para>
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
            // Disable HybridCache's own local cache outright. Flags merge per call (a per-call options object
            // with Flags == null inherits these), unlike LocalCacheExpiration, which a per-call Expiration
            // silently overrides. RedisNearCache's tracked L1 behind IDistributedCache is the only local tier.
            options.DefaultEntryOptions = new HybridCacheEntryOptions { Flags = HybridCacheEntryFlags.DisableLocalCache };
            configure?.Invoke(options);
        });

        return services;
    }
}
