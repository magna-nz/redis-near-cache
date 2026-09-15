using RedisNearCache.Tests;
using Xunit;
using Xunit.Abstractions;

namespace RedisNearCache.Tests.HybridCache;

/// <summary>
/// Cache-aside lost update at the <c>HybridCache</c> layer: a reader's factory loads the source, a writer then
/// updates the source and calls <c>HybridCache.SetAsync</c>, and only afterwards does the reader's write of its
/// (now old) factory result reach the distributed tier, overwriting the writer's newer value.
/// This pins a documented limitation (remarks on <c>AddRedisNearCacheHybridCache</c>): if it starts failing,
/// HybridCache or the adapter has changed how that write lands, and the documentation needs revisiting.
/// </summary>
public class HybridCacheWriteBackRaceTests : IClassFixture<HybridCacheFixture>
{
    private readonly HybridCacheFixture _fx;
    private readonly ITestOutputHelper _output;

    public HybridCacheWriteBackRaceTests(HybridCacheFixture fx, ITestOutputHelper output)
    {
        _fx = fx;
        _output = output;
    }

    [Fact]
    public async Task FactoryWriteBack_LandingAfterConcurrentSetAsync_OverwritesNewerValue()
    {
        var key = TestHelpers.Key("hc-writeback-race");
        var source = "v5";
        var factoryReadSource = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseFactory = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        var writer = await HybridCacheFixture.CreateAsync();
        var observer = await HybridCacheFixture.CreateAsync();
        try
        {
            // Reader (the fixture's provider): misses, loads v5 from the source, then stalls before returning it.
            var readerTask = _fx.HybridCache.GetOrCreateAsync<string>(key, async ct =>
            {
                var loaded = Volatile.Read(ref source);
                factoryReadSource.SetResult();
                await releaseFactory.Task.WaitAsync(ct);
                return loaded;
            }).AsTask();

            await factoryReadSource.Task.WaitAsync(TimeSpan.FromSeconds(5));

            // Writer (another process): updates the source, then the cache, both steps complete.
            Volatile.Write(ref source, "v6");
            await writer.HybridCache.SetAsync(key, "v6");
            Assert.Equal("v6", await observer.HybridCache.GetOrCreateAsync<string>(key, MustNotRun));

            // Now the reader's factory returns, and HybridCache persists its result.
            releaseFactory.SetResult();
            Assert.Equal("v5", await readerTask.WaitAsync(TimeSpan.FromSeconds(5)));

            string? seen = null;
            var overwritten = await Poll.UntilAsync(async () =>
            {
                seen = await observer.HybridCache.GetOrCreateAsync<string>(key, ReadSource);
                return seen == "v5";
            });
            _output.WriteLine($"observer saw {seen}; PTTL {await RedisCli.RunAsync(RedisCli.StandaloneContainer, "PTTL", key)} ms");

            Assert.True(overwritten, $"the write-back did not overwrite the newer value (observer saw {seen})");

            // Every instance keeps serving the old value although the source says v6 (checked for ~200 ms; the key
            // still carries its TTL, so it stays until it expires or is written again).
            for (var i = 0; i < 10; i++)
            {
                Assert.Equal("v5", await writer.HybridCache.GetOrCreateAsync<string>(key, ReadSource));
                Assert.Equal("v5", await _fx.HybridCache.GetOrCreateAsync<string>(key, ReadSource));
                await Task.Delay(20);
            }
            Assert.True(long.Parse(await RedisCli.RunAsync(RedisCli.StandaloneContainer, "PTTL", key)) > 0);
        }
        finally
        {
            releaseFactory.TrySetResult();
            await writer.DisposeAsync();
            await observer.DisposeAsync();
            RedisCli.Standalone("DEL", key);
        }

        ValueTask<string> ReadSource(CancellationToken ct) => ValueTask.FromResult(Volatile.Read(ref source));

        static ValueTask<string> MustNotRun(CancellationToken ct) =>
            throw new InvalidOperationException("the writer's value should be present; the factory must not run.");
    }
}
