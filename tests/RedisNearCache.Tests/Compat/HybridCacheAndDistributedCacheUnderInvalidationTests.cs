using System.Text;
using Microsoft.Extensions.Caching.Distributed;
using RedisNearCache.Tests.HybridCache;
using Xunit;
using Xunit.Abstractions;

namespace RedisNearCache.Tests.Compat;

/// <summary>
/// The standard caching abstractions on top of RedisNearCache, invalidated by a client that knows nothing
/// about either of them (redis-cli).
///
/// <para>
/// An external <c>SET</c> of a <c>HybridCache</c> entry's underlying key is deliberately NOT used here:
/// <c>HybridCache</c> writes its own framed payload through <see cref="Microsoft.Extensions.Caching.Distributed.IDistributedCache"/>,
/// and a foreign plain-string value at the same key is not decodable by its serializer. The realistic foreign
/// operation against a <c>HybridCache</c> entry is a <c>DEL</c>, which is what this test does; the factory then
/// has to run again. <see cref="Microsoft.Extensions.Caching.Distributed.IDistributedCache"/>, whose values are
/// raw bytes, is tested with an external <c>SET</c> as well.
/// </para>
/// </summary>
public class HybridCacheAndDistributedCacheUnderInvalidationTests : IClassFixture<HybridCacheFixture>
{
    private readonly HybridCacheFixture _fx;
    private readonly ITestOutputHelper _out;

    public HybridCacheAndDistributedCacheUnderInvalidationTests(HybridCacheFixture fx, ITestOutputHelper output)
    {
        _fx = fx;
        _out = output;
    }

    [Fact]
    public async Task HybridCacheFactoryRunsAgainAfterAnExternalDelete()
    {
        var key = TestHelpers.Key("hc-compat");
        var factoryCalls = 0;

        ValueTask<string> Factory(CancellationToken ct)
        {
            Interlocked.Increment(ref factoryCalls);
            return ValueTask.FromResult("v1");
        }

        try
        {
            Assert.Equal("v1", await _fx.HybridCache.GetOrCreateAsync<string>(key, Factory));

            // HybridCache persists a newly computed value to the distributed tier in the background, so the
            // entry has to settle before "the factory did not run" means anything (see the note in
            // AddRedisNearCacheHybridCache's remarks).
            var settled = await Poll.UntilAsync(async () =>
            {
                var before = Volatile.Read(ref factoryCalls);
                var value = await _fx.HybridCache.GetOrCreateAsync<string>(key, Factory);
                return value == "v1" && Volatile.Read(ref factoryCalls) == before;
            });
            Assert.True(settled, "the HybridCache entry never settled into being served without the factory.");
            var settledCalls = Volatile.Read(ref factoryCalls);

            // The entry really is a Redis key of that exact name; if this ever stops holding, the DEL below
            // would be a no-op and the rest of the test would prove nothing.
            Assert.Equal("1", RedisCli.Standalone("EXISTS", key));

            RedisCli.Standalone("DEL", key);

            var recomputed = await Poll.UntilAsync(async () =>
            {
                await _fx.HybridCache.GetOrCreateAsync<string>(key, Factory);
                return Volatile.Read(ref factoryCalls) > settledCalls;
            }, TimeSpan.FromSeconds(3));

            Assert.True(
                recomputed,
                "an external redis-cli DEL did not make HybridCache's factory run again within 3 s: the near cache kept serving a key that no longer exists.");
            _out.WriteLine($"factory ran {factoryCalls} times in total ({settledCalls} before the external DEL)");
        }
        finally
        {
            RedisCli.Standalone("DEL", key);
        }
    }

    [Fact]
    public async Task DistributedCacheReflectsAnExternalSet()
    {
        var key = TestHelpers.Key("dc-compat");
        try
        {
            await _fx.DistributedCache.SetAsync(key, Encoding.UTF8.GetBytes("v1"));
            Assert.Equal("v1", Encoding.UTF8.GetString((await _fx.DistributedCache.GetAsync(key))!));

            // Served from the tracked L1 now; the external write is the only thing that can dislodge it.
            var hitsBefore = _fx.Cache.Statistics.Hits;
            await _fx.DistributedCache.GetAsync(key);
            Assert.True(_fx.Cache.Statistics.Hits > hitsBefore, "the second IDistributedCache read did not come from L1.");

            RedisCli.Standalone("SET", key, "v2-from-outside");

            byte[]? seen = null;
            var updated = await Poll.UntilAsync(async () =>
            {
                seen = await _fx.DistributedCache.GetAsync(key);
                return seen is not null && Encoding.UTF8.GetString(seen) == "v2-from-outside";
            }, TimeSpan.FromSeconds(3));

            Assert.True(updated, "IDistributedCache did not reflect an external SET within 3 s.");
        }
        finally
        {
            RedisCli.Standalone("DEL", key);
        }
    }
}
