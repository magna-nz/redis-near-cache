using BenchmarkDotNet.Attributes;
using RedisNearCache.Bench.Contenders;

namespace RedisNearCache.Bench.Micro;

/// <summary>
/// Hot hit path: the key is already in the contender's local tier before any timed call (for <see cref="ContenderKind.Plain"/>,
/// which has no local tier, this is just a <c>GET</c> -- the floor every other kind is compared against).
/// <para>
/// Generic over the payload (<see cref="SampleRecord"/>/see <c>string</c> via <c>[GenericTypeArguments]</c>) rather
/// than a <c>[Params]</c> property: a runtime payload branch inside the timed call would add a comparison to every
/// contender's measured time, however small, and this class's whole point is measuring calls as small as ~1 ns.
/// </para>
/// <para>
/// Each <c>[Benchmark]</c> method has its own <c>[GlobalSetup(Target=...)]</c>/<c>[GlobalCleanup(Target=...)]</c>
/// pair that constructs and disposes ONLY the one contender that method times -- so no other kind's contender
/// (connections, RedisNearCache's 5 s re-arm sweep, FusionCache's backplane subscription, ...) is ever alive
/// alongside the one being measured, regardless of toolchain. (BenchmarkDotNet's out-of-process toolchain already
/// gives one process per case, which would have made this true anyway; per-target setup also makes it true if that
/// toolchain is ever unavailable and cases share a process.)
/// </para>
/// <para>
/// Benchmarks return the contender's own <see cref="ValueTask{TResult}"/> directly -- no <c>async</c>/<c>await</c>
/// wrapper -- so a synchronously-completing local hit allocates nothing and costs nothing beyond the library's own
/// work: an <c>async Task&lt;object?&gt;</c> wrapper would add a state machine and a boxed/heap-allocated Task to
/// every call, which is significant next to a ~1 ns hit and would show up in <c>MemoryDiagnoser</c>'s Allocated
/// column as harness noise, not the library's own cost.
/// </para>
/// <see cref="ContenderKind.Plain"/> is the baseline (<c>Baseline = true</c>).
/// </summary>
[MemoryDiagnoser]
[GenericTypeArguments(typeof(string))]
[GenericTypeArguments(typeof(SampleRecord))]
public class HitBenchmarks<T>
    where T : class
{
    private static string Key => $"micro:hit:{(typeof(T) == typeof(string) ? DataKind.String : DataKind.Json)}";

    private ContenderSettings _settings = null!;
    private IContender _contender = null!;

    /// <summary>Connects one contender, seeds the shared key from outside any contender, reads it once (populates
    /// the local tier / L2), then asserts a subsequent read completes synchronously when the contender promises it
    /// does.</summary>
    private async Task SetupAsync(ContenderKind kind)
    {
        _settings = MicroConfig.BuildSettings("bdn-hit", TimeSpan.FromSeconds(30));

        await using (var seeder = await ContenderRedis.ConnectAsync(_settings, "bdn-hit-seed", allowAdmin: true))
        {
            object seed = typeof(T) == typeof(string) ? Payloads.StringValue : Payloads.JsonValue;
            await seeder.GetDatabase().StringSetAsync(Key, ContenderRedis.Encode(seed));
        }

        var contender = ContenderFactory.Create(kind, _settings with { InstanceName = $"hit-{kind}" });
        await contender.InitializeAsync();

        _ = await contender.GetAsync<T>(Key);
        await AssertSynchronousHitAsync(contender, kind);

        _contender = contender;
    }

    /// <summary>
    /// Reads the primed key and asserts synchronous completion for contenders that promise it. A single observed
    /// non-synchronous read is not immediately treated as a broken promise: releasing an internal per-key lock after
    /// populating on a miss is itself asynchronous in some libraries (verified directly against FusionCache 2.8.0,
    /// which completes synchronously reliably once that release has happened), so this retries briefly before
    /// failing -- a contender that never once completes synchronously within the budget still fails loudly.
    /// </summary>
    private static async Task AssertSynchronousHitAsync(IContender contender, ContenderKind kind)
    {
        if (!contender.LocalHitsCompleteSynchronously) return;

        const int maxAttempts = 50;
        for (var attempt = 1; attempt <= maxAttempts; attempt++)
        {
            var vt = contender.GetAsync<T>(Key);
            var synchronous = vt.IsCompletedSuccessfully;
            _ = await vt;

            if (synchronous) return;
            if (attempt < maxAttempts) await Task.Delay(10);
        }

        throw new InvalidOperationException(
            $"{kind}: LocalHitsCompleteSynchronously is true but no read of a primed key completed synchronously in {maxAttempts} attempts");
    }

    private async Task CleanupAsync()
    {
        await _contender.DisposeAsync();

        await using var admin = await ContenderRedis.ConnectAsync(_settings, "bdn-hit-cleanup", allowAdmin: true);
        var db = admin.GetDatabase();
        await db.KeyDeleteAsync(Key);
        foreach (var server in admin.GetServers().Where(s => s.IsConnected))
        {
            await foreach (var owned in server.KeysAsync(pattern: ContenderRedis.OwnedKeyPrefix + "*", pageSize: 1000))
            {
                await db.KeyDeleteAsync(owned);
            }
        }
    }

    [GlobalSetup(Target = nameof(Plain))]
    public Task SetupPlain() => SetupAsync(ContenderKind.Plain);

    [GlobalCleanup(Target = nameof(Plain))]
    public Task CleanupPlain() => CleanupAsync();

    [Benchmark(Baseline = true)]
    public ValueTask<T?> Plain() => _contender.GetAsync<T>(Key);

    [GlobalSetup(Target = nameof(MemoryCacheTtl))]
    public Task SetupMemoryCacheTtl() => SetupAsync(ContenderKind.MemoryCacheTtl);

    [GlobalCleanup(Target = nameof(MemoryCacheTtl))]
    public Task CleanupMemoryCacheTtl() => CleanupAsync();

    [Benchmark]
    public ValueTask<T?> MemoryCacheTtl() => _contender.GetAsync<T>(Key);

    [GlobalSetup(Target = nameof(HybridCache))]
    public Task SetupHybridCache() => SetupAsync(ContenderKind.HybridCache);

    [GlobalCleanup(Target = nameof(HybridCache))]
    public Task CleanupHybridCache() => CleanupAsync();

    [Benchmark]
    public ValueTask<T?> HybridCache() => _contender.GetAsync<T>(Key);

    [GlobalSetup(Target = nameof(FusionCache))]
    public Task SetupFusionCache() => SetupAsync(ContenderKind.FusionCache);

    [GlobalCleanup(Target = nameof(FusionCache))]
    public Task CleanupFusionCache() => CleanupAsync();

    [Benchmark]
    public ValueTask<T?> FusionCache() => _contender.GetAsync<T>(Key);

    [GlobalSetup(Target = nameof(NearCache))]
    public Task SetupNearCache() => SetupAsync(ContenderKind.NearCache);

    [GlobalCleanup(Target = nameof(NearCache))]
    public Task CleanupNearCache() => CleanupAsync();

    [Benchmark]
    public ValueTask<T?> NearCache() => _contender.GetAsync<T>(Key);

    [GlobalSetup(Target = nameof(NearCacheHybridCache))]
    public Task SetupNearCacheHybridCache() => SetupAsync(ContenderKind.NearCacheHybridCache);

    [GlobalCleanup(Target = nameof(NearCacheHybridCache))]
    public Task CleanupNearCacheHybridCache() => CleanupAsync();

    [Benchmark]
    public ValueTask<T?> NearCacheHybridCache() => _contender.GetAsync<T>(Key);
}
