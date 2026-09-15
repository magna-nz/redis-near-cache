using BenchmarkDotNet.Attributes;
using RedisNearCache.Bench.Contenders;

namespace RedisNearCache.Bench.Micro;

/// <summary>
/// Cold-read (miss) path: local miss, L2 miss where applicable, source load, populate. Each timed invocation reads a
/// key this contender instance has never read before and that has no L2 entry, drawn from a pre-seeded
/// <see cref="MissKeyPool"/> sized from <see cref="Program"/>'s fixed BenchmarkDotNet job (InvocationCount x
/// (WarmupCount + IterationCount), see <see cref="MicroJobSettings"/>). For <see cref="ContenderKind.Plain"/>, which
/// has no local tier, this is also just a <c>GET</c> -- the floor.
/// <para>
/// Generic over the payload rather than <c>[Params]</c>, and each <c>[Benchmark]</c> returns the contender's own
/// <see cref="ValueTask{TResult}"/> directly with a per-target <c>[GlobalSetup]</c>/<c>[GlobalCleanup]</c> pair --
/// see <see cref="HitBenchmarks{T}"/>'s remarks, which apply here identically. One contender per kind is created
/// once (in that kind's own setup) and reused across every invocation of that kind's method; freshness comes from
/// the pool (a different key every call), not from a fresh contender every call.
/// </para>
/// </summary>
[MemoryDiagnoser]
[GenericTypeArguments(typeof(string))]
[GenericTypeArguments(typeof(SampleRecord))]
public class MissBenchmarks<T>
    where T : class
{
    private static DataKind Payload => typeof(T) == typeof(string) ? DataKind.String : DataKind.Json;

    private ContenderSettings _settings = null!;
    private MissKeyPool _pool = null!;
    private IContender _contender = null!;

    private async Task SetupAsync(ContenderKind kind)
    {
        _settings = MicroConfig.BuildSettings("bdn-miss", TimeSpan.FromSeconds(30));
        var poolSize = MicroJobSettings.BdnMissPoolSize(MicroConfig.Quick);
        _pool = await MissKeyPool.CreateAsync(_settings, Payload, poolSize);

        var contender = ContenderFactory.Create(kind, _settings with { InstanceName = $"miss-{kind}" });
        await contender.InitializeAsync();
        _contender = contender;
    }

    private async Task CleanupAsync()
    {
        await _contender.DisposeAsync();
        await _pool.DisposeAsync();
    }

    [GlobalSetup(Target = nameof(Plain))]
    public Task SetupPlain() => SetupAsync(ContenderKind.Plain);

    [GlobalCleanup(Target = nameof(Plain))]
    public Task CleanupPlain() => CleanupAsync();

    [Benchmark(Baseline = true)]
    public ValueTask<T?> Plain() => _contender.GetAsync<T>(_pool.Next());

    [GlobalSetup(Target = nameof(MemoryCacheTtl))]
    public Task SetupMemoryCacheTtl() => SetupAsync(ContenderKind.MemoryCacheTtl);

    [GlobalCleanup(Target = nameof(MemoryCacheTtl))]
    public Task CleanupMemoryCacheTtl() => CleanupAsync();

    [Benchmark]
    public ValueTask<T?> MemoryCacheTtl() => _contender.GetAsync<T>(_pool.Next());

    [GlobalSetup(Target = nameof(HybridCache))]
    public Task SetupHybridCache() => SetupAsync(ContenderKind.HybridCache);

    [GlobalCleanup(Target = nameof(HybridCache))]
    public Task CleanupHybridCache() => CleanupAsync();

    [Benchmark]
    public ValueTask<T?> HybridCache() => _contender.GetAsync<T>(_pool.Next());

    [GlobalSetup(Target = nameof(FusionCache))]
    public Task SetupFusionCache() => SetupAsync(ContenderKind.FusionCache);

    [GlobalCleanup(Target = nameof(FusionCache))]
    public Task CleanupFusionCache() => CleanupAsync();

    [Benchmark]
    public ValueTask<T?> FusionCache() => _contender.GetAsync<T>(_pool.Next());

    [GlobalSetup(Target = nameof(NearCache))]
    public Task SetupNearCache() => SetupAsync(ContenderKind.NearCache);

    [GlobalCleanup(Target = nameof(NearCache))]
    public Task CleanupNearCache() => CleanupAsync();

    [Benchmark]
    public ValueTask<T?> NearCache() => _contender.GetAsync<T>(_pool.Next());

    [GlobalSetup(Target = nameof(NearCacheHybridCache))]
    public Task SetupNearCacheHybridCache() => SetupAsync(ContenderKind.NearCacheHybridCache);

    [GlobalCleanup(Target = nameof(NearCacheHybridCache))]
    public Task CleanupNearCacheHybridCache() => CleanupAsync();

    [Benchmark]
    public ValueTask<T?> NearCacheHybridCache() => _contender.GetAsync<T>(_pool.Next());
}
