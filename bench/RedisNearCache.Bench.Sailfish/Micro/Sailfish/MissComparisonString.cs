using RedisNearCache.Bench.Contenders;
using Sailfish.Attributes;

namespace RedisNearCache.Bench.Micro.Comparisons;

/// <summary>
/// Per-call cold-read (miss) latency distribution, 1 KB string payload. Fixed sample size (no adaptive sampling):
/// every sample must draw a fresh, never-before-read key from a pool sized ahead of time (see
/// <see cref="MicroJobSettings.SailfishMissPoolSize"/>), so the sample count has to be exact rather than
/// open-ended. See <see cref="HitComparisonString"/> for why payload is a separate class.
/// </summary>
[Sailfish(sampleSize: MicroJobSettings.SailfishMissSampleSize, numWarmupIterations: MicroJobSettings.SailfishMissWarmupIterations)]
[WriteToCsv]
[WriteToMarkdown]
public class MissComparisonString : SailfishMissComparisonBase
{
    protected override DataKind Payload => DataKind.String;

    [SailfishGlobalSetup]
    public async Task GlobalSetup() => await GlobalSetupCore();

    [SailfishGlobalTeardown]
    public async Task GlobalTeardown() => await GlobalTeardownCore();

    [SailfishMethodSetup(nameof(Plain))]
    public async Task SetupPlain() => await SetupMethod(ContenderKind.Plain);

    [SailfishMethodTeardown(nameof(Plain))]
    public async Task TeardownPlain() => await TeardownMethod();

    [SailfishMethod(IsBaseline = true)]
    public async Task<object?> Plain() => await ReadAsync();

    [SailfishMethodSetup(nameof(MemoryCacheTtl))]
    public async Task SetupMemoryCacheTtl() => await SetupMethod(ContenderKind.MemoryCacheTtl);

    [SailfishMethodTeardown(nameof(MemoryCacheTtl))]
    public async Task TeardownMemoryCacheTtl() => await TeardownMethod();

    [SailfishMethod]
    public async Task<object?> MemoryCacheTtl() => await ReadAsync();

    [SailfishMethodSetup(nameof(HybridCache))]
    public async Task SetupHybridCache() => await SetupMethod(ContenderKind.HybridCache);

    [SailfishMethodTeardown(nameof(HybridCache))]
    public async Task TeardownHybridCache() => await TeardownMethod();

    [SailfishMethod]
    public async Task<object?> HybridCache() => await ReadAsync();

    [SailfishMethodSetup(nameof(FusionCache))]
    public async Task SetupFusionCache() => await SetupMethod(ContenderKind.FusionCache);

    [SailfishMethodTeardown(nameof(FusionCache))]
    public async Task TeardownFusionCache() => await TeardownMethod();

    [SailfishMethod]
    public async Task<object?> FusionCache() => await ReadAsync();

    [SailfishMethodSetup(nameof(NearCache))]
    public async Task SetupNearCache() => await SetupMethod(ContenderKind.NearCache);

    [SailfishMethodTeardown(nameof(NearCache))]
    public async Task TeardownNearCache() => await TeardownMethod();

    [SailfishMethod]
    public async Task<object?> NearCache() => await ReadAsync();

    [SailfishMethodSetup(nameof(NearCacheHybridCache))]
    public async Task SetupNearCacheHybridCache() => await SetupMethod(ContenderKind.NearCacheHybridCache);

    [SailfishMethodTeardown(nameof(NearCacheHybridCache))]
    public async Task TeardownNearCacheHybridCache() => await TeardownMethod();

    [SailfishMethod]
    public async Task<object?> NearCacheHybridCache() => await ReadAsync();
}
