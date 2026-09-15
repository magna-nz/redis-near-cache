using System.Net;
using StackExchange.Redis;
using Xunit;

namespace RedisNearCache.Tests.Broadcast;

public class ClusterPerNodeInvalidationTests : IClassFixture<BroadcastClusterCacheFixture>
{
    private readonly BroadcastClusterCacheFixture _fx;

    public ClusterPerNodeInvalidationTests(BroadcastClusterCacheFixture fx) => _fx = fx;

    [Fact]
    public async Task ForeignWriteOnEachMasterEvictsItsKey()
    {
        var mux = _fx.Connection.Multiplexer;
        var endpoints = mux.GetEndPoints();
        var anyServer = mux.GetServer(endpoints[0]);

        var keyByEndpoint = KeyPerMasterUnderPrefix(mux, anyServer, endpoints);
        Assert.Equal(BroadcastClusterCacheFixture.MasterPorts.Length, keyByEndpoint.Count);

        foreach (var (endpoint, key) in keyByEndpoint)
        {
            var port = ((IPEndPoint)endpoint).Port;

            await _fx.Cache.SetAsync(key, "v1");
            // A Broadcast socket is armed for the whole prefix regardless of who reads what, so this SetAsync
            // already generated a self-invalidation before the key was ever read: poll rather than asserting the
            // very next GetAsync is cached synchronously (see the remarks on ExternalWriteEvictsTests).
            Assert.True(await TestHelpers.ReadUntilCachedAsync(_fx.Cache, key, "v1"), $"{key} on master port {port} was not cached after SetAsync+GetAsync.");

            RedisCli.Cluster(port, "SET", key, "v2");

            var evicted = await Poll.UntilAsync(() => !_fx.Cache.TryGetLocal<string>(key, out _));
            Assert.True(evicted, $"key on master port {port} was not evicted after external write.");
        }

        Assert.Equal(BroadcastClusterCacheFixture.MasterPorts.Length, _fx.Armer.RedirectTargets.Count);
    }

    /// <summary>
    /// Same idea as <see cref="TestHelpers.KeyPerMaster"/>, but generating keys under <see cref="BroadcastKey.Prefix"/>
    /// so the broadcast socket (armed with that prefix only) actually receives the invalidation.
    /// </summary>
    private static Dictionary<EndPoint, string> KeyPerMasterUnderPrefix(
        IConnectionMultiplexer mux, IServer anyServer, IReadOnlyCollection<EndPoint> endpoints)
    {
        var result = new Dictionary<EndPoint, string>();
        for (var i = 0; i < 40_000 && result.Count < endpoints.Count; i++)
        {
            var candidate = BroadcastKey.New($"node{i}");
            var slot = mux.GetHashSlot(candidate);
            var node = anyServer.ClusterNodes()?.GetBySlot(slot);
            if (node is null) continue;
            var match = endpoints.FirstOrDefault(e => e.Equals(node.EndPoint));
            if (match is not null && !result.ContainsKey(match)) result[match] = candidate;
        }

        return result;
    }
}
