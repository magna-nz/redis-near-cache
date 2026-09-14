using RedisNearCache.Bench.Contenders;
using Sailfish.Attributes;

namespace RedisNearCache.Bench.Micro.Comparisons;

/// <summary>Per-call hit-path latency distribution, small JSON payload. See <see cref="HitComparisonString"/> for why
/// payload is a separate class rather than a <c>[SailfishVariable]</c>. Excluded under <c>--quick</c> (String only).</summary>
[Sailfish(
    sampleSize: MicroJobSettings.SailfishHitMaximumSampleSize,
    numWarmupIterations: MicroJobSettings.SailfishHitWarmupIterations,
    UseAdaptiveSampling = true,
    TargetCoefficientOfVariation = MicroJobSettings.SailfishHitTargetCoefficientOfVariation,
    MinimumSampleSize = MicroJobSettings.SailfishHitMinimumSampleSize,
    MaximumSampleSize = MicroJobSettings.SailfishHitMaximumSampleSize)]
[WriteToCsv]
[WriteToMarkdown]
public class HitComparisonJson : SailfishHitComparisonBase
{
    protected override DataKind Payload => DataKind.Json;

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
