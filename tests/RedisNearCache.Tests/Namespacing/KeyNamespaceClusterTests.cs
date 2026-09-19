using System.Net;
using RedisNearCache.Tests.EdgeCases;
using StackExchange.Redis;
using Xunit;

namespace RedisNearCache.Tests.Namespacing;

/// <summary>
/// A namespaced cache on the 3-master cluster. The namespace is part of the key Redis sees, so it is the FULL key
/// that decides the slot and therefore the node - which means both that a caller key has to be routed by its full
/// key (otherwise reads would go to the wrong node and be answered with MOVED, or worse, tracked on a node that
/// never holds the key), and that a caller's hash tag still works, because <c>{tag}</c> inside the caller key is
/// still <c>{tag}</c> inside the full key.
/// </summary>
public class KeyNamespaceClusterTests
{
    /// <summary>
    /// One caller key per master, chosen so that <c>namespace + key</c> - the key Redis actually stores - hashes to
    /// that master. <see cref="TestHelpers.KeyPerMaster"/> hashes the key as given, which is the wrong key here.
    /// </summary>
    private static Dictionary<EndPoint, string> CallerKeyPerMaster(
        IConnectionMultiplexer mux, IServer anyServer, IReadOnlyCollection<EndPoint> endpoints, string keyNamespace)
    {
        var result = new Dictionary<EndPoint, string>();
        for (var i = 0; i < 40_000 && result.Count < endpoints.Count; i++)
        {
            var candidate = TestHelpers.Key($"ns-node{i}");
            var node = anyServer.ClusterNodes()?.GetBySlot(mux.GetHashSlot(keyNamespace + candidate));
            if (node is null) continue;
            var match = endpoints.FirstOrDefault(e => e.Equals(node.EndPoint));
            if (match is not null && !result.ContainsKey(match)) result[match] = candidate;
        }

        return result;
    }

    [Fact]
    public async Task NamespacedKeysAreRoutedCachedAndInvalidatedPerNode()
    {
        var ns = $"ns-{Guid.NewGuid():N}:";
        var handle = await EdgeCaseSupport.BuildAsync(ClusterCacheFixture.ConnectionString, o => o.KeyNamespace = ns);
        var created = new List<string>();
        try
        {
            var cache = handle.Cache;
            var mux = handle.Multiplexer;
            var endpoints = mux.GetEndPoints();
            var keyByEndpoint = CallerKeyPerMaster(mux, mux.GetServer(endpoints[0]), endpoints, ns);
            Assert.Equal(3, keyByEndpoint.Count);

            foreach (var (endpoint, key) in keyByEndpoint)
            {
                var port = ((IPEndPoint)endpoint).Port;
                var full = ns + key;
                created.Add(full);

                await cache.SetAsync(key, "v1");
                Assert.Equal("v1", RedisCli.Cluster(port, "GET", full));
                Assert.True(await TestHelpers.ReadUntilCachedAsync(cache, key, "v1"),
                    $"the key on master {port} was never cached.");

                RedisCli.Cluster(port, "SET", full, "v2");

                Assert.True(await Poll.UntilAsync(() => !cache.TryGetLocal<string>(key, out _), TimeSpan.FromSeconds(5)),
                    $"the key on master port {port} was not evicted after an external write to its full key.");
                Assert.Equal("v2", await cache.GetAsync<string>(key));
            }

            Assert.Equal(3, handle.Armer.RedirectTargets.Count);
        }
        finally
        {
            await handle.DisposeAsync();
            foreach (var full in created) RedisCli.Cluster(7100, "DEL", full);
        }
    }

    /// <summary>
    /// The caller's hash tag still puts two keys on one slot, because the namespace goes in FRONT of it and a hash
    /// tag is the first <c>{...}</c> in the key wherever it sits. Read together, on one node, and invalidated
    /// individually.
    /// </summary>
    [Fact]
    public async Task ACallersHashTagStillSharesASlotUnderANamespace()
    {
        var ns = $"ns-{Guid.NewGuid():N}:";
        var tag = $"{{tag-{Guid.NewGuid():N}}}";
        var first = $"{tag}:a";
        var second = $"{tag}:b";
        var firstFull = ns + first;
        var secondFull = ns + second;
        var handle = await EdgeCaseSupport.BuildAsync(ClusterCacheFixture.ConnectionString, o => o.KeyNamespace = ns);
        try
        {
            var cache = handle.Cache;
            var mux = handle.Multiplexer;

            Assert.Equal(mux.GetHashSlot(firstFull), mux.GetHashSlot(secondFull));
            // ...and it is the FULL keys that share it, not merely the caller's.
            Assert.Equal(mux.GetHashSlot(first), mux.GetHashSlot(firstFull));

            await cache.SetAsync(first, "a1");
            await cache.SetAsync(second, "b1");
            Assert.Equal("a1", RedisCli.Cluster(7100, "GET", firstFull));
            Assert.Equal("b1", RedisCli.Cluster(7100, "GET", secondFull));

            var many = await cache.GetManyAsync<string>([first, second]);
            Assert.Equal("a1", many[first]);
            Assert.Equal("b1", many[second]);

            Assert.True(await TestHelpers.ReadUntilCachedAsync(cache, first, "a1"));
            Assert.True(await TestHelpers.ReadUntilCachedAsync(cache, second, "b1"));

            RedisCli.Cluster(7100, "SET", firstFull, "a2");

            Assert.True(await Poll.UntilAsync(() => !cache.TryGetLocal<string>(first, out _), TimeSpan.FromSeconds(5)),
                "the first key of the tagged pair was not evicted.");
            Assert.True(cache.TryGetLocal<string>(second, out _),
                "the second key of the tagged pair shares a slot but is a different key: it must not have been evicted.");
            Assert.Equal("a2", await cache.GetAsync<string>(first));
        }
        finally
        {
            await handle.DisposeAsync();
            RedisCli.Cluster(7100, "DEL", firstFull);
            RedisCli.Cluster(7100, "DEL", secondFull);
        }
    }
}
