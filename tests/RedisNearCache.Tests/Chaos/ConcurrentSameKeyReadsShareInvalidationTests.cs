using Microsoft.Extensions.DependencyInjection;
using Xunit;
using Xunit.Abstractions;

namespace RedisNearCache.Tests.Chaos;

/// <summary>
/// Eight reads of the same key are parked, all of them past the Redis reply and none of them having stored
/// it yet; the key is then invalidated from outside; then all eight are released at once. Every one of them
/// is holding a value the server has already superseded, so every one of them must discard it.
/// <para>
/// The in-flight tracker gives each concurrent read of a key its own token but one shared per-key
/// "last invalidated" version (src/RedisNearCache/Caching/InFlightTracker.cs:110-121), so this is the test of
/// whether one invalidation really covers all eight readers, or only the one that happened to register last.
/// </para>
/// Needs its own provider for the internal <see cref="RedisNearCacheOptions.TestHooks"/> gate.
/// </summary>
public class ConcurrentSameKeyReadsShareInvalidationTests : IAsyncLifetime
{
    private const int Readers = 8;

    private readonly ITestOutputHelper _out;
    private ServiceProvider _provider = null!;
    private IRedisNearCache _cache = null!;
    private RedisNearCacheOptions _options = null!;

    public ConcurrentSameKeyReadsShareInvalidationTests(ITestOutputHelper output) => _out = output;

    public async Task InitializeAsync()
    {
        var services = new ServiceCollection();
        services.AddRedisNearCache(StandaloneCacheFixture.ConnectionString, o => _options = o);
        _provider = services.BuildServiceProvider();
        _cache = _provider.GetRequiredService<IRedisNearCache>();
        await _cache.Ready;
    }

    public async Task DisposeAsync()
    {
        _options.TestHooks.AfterRedisReadBeforeStore = null;
        await _provider.DisposeAsync();
    }

    [Fact]
    public async Task ConcurrentSameKeyReadsShareInvalidation()
    {
        var key = TestHelpers.Key("shared-inflight");
        await _cache.SetAsync(key, "v1");

        var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var parked = 0;

        _options.TestHooks.AfterRedisReadBeforeStore = async k =>
        {
            if (!string.Equals(k, key, StringComparison.Ordinal)) return;
            Interlocked.Increment(ref parked);
            await gate.Task;
        };

        var reads = Enumerable.Range(0, Readers)
            .Select(_ => Task.Run(async () => await _cache.GetAsync<string>(key)))
            .ToArray();

        var allParked = await Poll.UntilAsync(() => Volatile.Read(ref parked) == Readers, TimeSpan.FromSeconds(10));
        if (!allParked) gate.TrySetResult();
        Assert.True(allParked, $"only {Volatile.Read(ref parked)} of {Readers} reads reached the pre-store hook.");

        Assert.False(_cache.TryGetLocal<string>(key, out _), "nothing should be in L1 while every read is parked before its store.");

        var raceDiscardsBefore = _cache.Statistics.RaceDiscards;
        var invalidationsBefore = _cache.Statistics.Invalidations;

        RedisCli.Standalone("SET", key, "v2");

        var invalidated = await Poll.UntilAsync(
            () => _cache.Statistics.Invalidations > invalidationsBefore, TimeSpan.FromSeconds(10));
        if (!invalidated) gate.TrySetResult();
        Assert.True(invalidated, "the external write produced no invalidation while the reads were parked.");

        gate.SetResult();
        var values = await Task.WhenAll(reads);

        var raceDiscards = _cache.Statistics.RaceDiscards - raceDiscardsBefore;
        _out.WriteLine($"{Readers} parked reads returned [{string.Join(",", values)}]; raceDiscards={raceDiscards}; stats={_cache.Statistics}");

        // Each read legitimately RETURNS the value it read ("v1"); what must not happen is that value being
        // left in L1, where a later reader would be handed it long after the server superseded it.
        Assert.False(_cache.TryGetLocal<string>(key, out var leftBehind),
            $"a reply invalidated while in flight populated L1 with '{leftBehind}'; Redis holds 'v2'.");
        Assert.True(raceDiscards >= 1, $"expected at least one RaceDiscard, got {raceDiscards}.");

        _options.TestHooks.AfterRedisReadBeforeStore = null;
        Assert.Equal("v2", await _cache.GetAsync<string>(key));
    }
}
