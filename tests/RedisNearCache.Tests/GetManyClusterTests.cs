using System.Net;
using RedisNearCache.Tests.EdgeCases;
using Xunit;
using Xunit.Abstractions;

namespace RedisNearCache.Tests;

/// <summary>
/// <see cref="IRedisNearCache.GetManyAsync{T}"/> across a 3-master cluster. Because the fan-out issues one
/// single-key read per key, each is routed by its own slot: keys owned by three different masters (and therefore
/// tracked by three separate <c>CLIENT TRACKING ... REDIRECT</c> arms) come back in one call, as do same-slot
/// hash-tagged keys, and a foreign write on one master evicts only the key it wrote. An <c>MGET</c> over these
/// keys would be refused outright (<c>CROSSSLOT</c>), which is what
/// <see cref="Compat.HashTagsAndMultiKeyReadsOnClusterTests"/> demonstrates.
/// </summary>
public class GetManyClusterTests
{
    private readonly ITestOutputHelper _out;

    public GetManyClusterTests(ITestOutputHelper output) => _out = output;

    [Fact]
    public async Task KeysOnEveryMasterAndASameSlotPairComeBackInOneCall()
    {
        var handle = await EdgeCaseSupport.BuildAsync(ClusterCacheFixture.ConnectionString);
        var tag = $"gmtag{Guid.NewGuid():N}";
        var all = Array.Empty<string>();
        try
        {
            var cache = handle.Cache;
            var mux = handle.Multiplexer;
            var endpoints = mux.GetEndPoints();
            var keyByEndpoint = TestHelpers.KeyPerMaster(mux, mux.GetServer(endpoints[0]), endpoints);
            Assert.Equal(3, keyByEndpoint.Count);

            var perMaster = keyByEndpoint.Values.ToArray();
            string[] tagged = [$"{{{tag}}}:a", $"{{{tag}}}:b"];
            Assert.Equal(mux.GetHashSlot(tagged[0]), mux.GetHashSlot(tagged[1]));
            var slots = perMaster.Select(k => mux.GetHashSlot(k)).ToArray();
            Assert.Equal(3, slots.Distinct().Count());
            _out.WriteLine($"per-master keys on slots {string.Join(", ", slots)}; tagged pair on slot {mux.GetHashSlot(tagged[0])}");

            all = [.. perMaster, .. tagged];
            for (var i = 0; i < all.Length; i++) RedisCli.Cluster(ClusterCacheFixture.MasterPorts[0], "SET", all[i], $"v{i}");

            var misses = cache.Statistics.Misses;
            var first = await cache.GetManyAsync<string>(all);

            Assert.Equal(all.Length, first.Count);
            for (var i = 0; i < all.Length; i++) Assert.Equal($"v{i}", first[all[i]]);
            Assert.Equal(misses + all.Length, cache.Statistics.Misses);

            var uncached = all.Where(k => !cache.TryGetLocal<string>(k, out _)).ToArray();
            Assert.True(uncached.Length == 0, $"keys not cached after the multi-key read: {string.Join(", ", uncached)}");

            // A foreign write routed to one master's key: only that key leaves L1, although the call that cached
            // them all spanned three masters' tracking tables.
            var (endpoint, written) = keyByEndpoint.First();
            var port = ((IPEndPoint)endpoint).Port;
            RedisCli.Cluster(port, "SET", written, "rewritten");

            var evicted = await Poll.UntilAsync(() => !cache.TryGetLocal<string>(written, out _), TimeSpan.FromSeconds(5));
            Assert.True(evicted, $"the key on master {port} was not evicted after a foreign write.");
            foreach (var other in all.Where(k => k != written))
            {
                Assert.True(cache.TryGetLocal<string>(other, out _), $"{other} was evicted although nothing wrote to it.");
            }

            var hits = cache.Statistics.Hits;
            misses = cache.Statistics.Misses;

            var second = await cache.GetManyAsync<string>(all);

            Assert.Equal("rewritten", second[written]);
            Assert.Equal(hits + all.Length - 1, cache.Statistics.Hits);
            Assert.Equal(misses + 1, cache.Statistics.Misses);
        }
        finally
        {
            foreach (var key in all) RedisCli.Cluster(ClusterCacheFixture.MasterPorts[0], "DEL", key);
            await handle.DisposeAsync();
        }
    }
}
