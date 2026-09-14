namespace RedisNearCache.Bench.Contenders;

/// <summary>Creates an uninitialized contender; call <see cref="IContender.InitializeAsync"/> before use.</summary>
public static class ContenderFactory
{
    public static IContender Create(ContenderKind kind, ContenderSettings settings)
    {
        ArgumentNullException.ThrowIfNull(settings);
        return kind switch
        {
            ContenderKind.Plain => new PlainContender(settings),
            ContenderKind.MemoryCacheTtl => new MemoryCacheTtlContender(settings),
            ContenderKind.HybridCache => new HybridCacheContender(settings),
            ContenderKind.FusionCache => new FusionCacheContender(settings),
            ContenderKind.NearCache => new NearCacheContender(settings),
            ContenderKind.NearCacheHybridCache => new NearCacheHybridCacheContender(settings),
            _ => throw new ArgumentOutOfRangeException(nameof(kind), kind, null),
        };
    }

    /// <summary>True for the kinds built on RedisNearCache (chaos mode and the invalidation counters apply to them).</summary>
    public static bool IsRedisNearCache(ContenderKind kind) =>
        kind is ContenderKind.NearCache or ContenderKind.NearCacheHybridCache;
}
