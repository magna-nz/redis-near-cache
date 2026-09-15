using RedisNearCache.Bench.Contenders;
using Sailfish.Attributes;

namespace RedisNearCache.Bench.Micro.Comparisons;

/// <summary>
/// Per-call hit-path latency distribution, 1 KB string payload, one <see cref="SailfishMethodAttribute"/> per
/// <see cref="ContenderKind"/>, <c>Plain</c> as the baseline. Adaptive sampling (bounded by
/// <see cref="MicroJobSettings"/>) so a fast in-process hit and a slow network round trip both get a stable median
/// without over- or under-sampling.
/// <para>
/// Payload is a separate class rather than a <c>[SailfishVariable]</c>-decorated property: with a
/// <c>[SailfishVariable]</c> in play, each variable value produces its own baseline test case, and Sailfish 4.0.221's
/// same-run method comparison falls back to full N x N pairwise rows (with a "N methods marked IsBaseline=true"
/// warning) instead of clean baseline-vs-other rows once a class has more than one baseline test case -- verified
/// against the installed package, not assumed. One payload per class keeps exactly one baseline test case per class,
/// which keeps the comparison CSV/markdown to clean baseline-vs-other rows.
/// </para>
/// </summary>
[Sailfish(
    sampleSize: MicroJobSettings.SailfishHitMaximumSampleSize,
    numWarmupIterations: MicroJobSettings.SailfishHitWarmupIterations,
    UseAdaptiveSampling = true,
    TargetCoefficientOfVariation = MicroJobSettings.SailfishHitTargetCoefficientOfVariation,
    MinimumSampleSize = MicroJobSettings.SailfishHitMinimumSampleSize,
    MaximumSampleSize = MicroJobSettings.SailfishHitMaximumSampleSize)]
[WriteToCsv]
[WriteToMarkdown]
public class HitComparisonString : SailfishHitComparisonBase
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
