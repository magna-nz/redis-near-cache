using System.Net;
using RedisNearCache.Internal;
using Xunit;

namespace RedisNearCache.Tests;

public class ClusterKillOneNodeInteractiveRearmsTests : IClassFixture<ClusterCacheFixture>
{
    private readonly ClusterCacheFixture _fx;

    public ClusterKillOneNodeInteractiveRearmsTests(ClusterCacheFixture fx) => _fx = fx;

    [Fact]
    public async Task ClusterKillOneNodeInteractiveRearms()
    {
        var mux = _fx.Connection.Multiplexer;
        var endpoints = mux.GetEndPoints();
        var targetEndpoint = endpoints.First(e => ((IPEndPoint)e).Port == ClusterCacheFixture.MasterPorts[0]);
        var targetPort = ((IPEndPoint)targetEndpoint).Port;
        var otherEndpoints = endpoints.Where(e => !e.Equals(targetEndpoint)).ToArray();

        // Snapshot the other nodes' redirect targets before touching the target node.
        var beforeOthers = otherEndpoints.ToDictionary(e => e, e => _fx.Armer.RedirectTargets[e]);

        var sawRestoreForTarget = false;
        _fx.Armer.Armed += e =>
        {
            if (e.Reason == ArmReason.InteractiveRestored && e.EndPoint.Equals(targetEndpoint))
                sawRestoreForTarget = true;
        };

        var targetServer = mux.GetServer(targetEndpoint);
        var interactiveId = (long)await targetServer.ExecuteAsync("CLIENT", "ID");
        RedisCli.Cluster(targetPort, "CLIENT", "KILL", "ID", interactiveId.ToString());

        var rearmed = await Poll.UntilAsync(
            () => sawRestoreForTarget && _fx.Armer.RedirectTargets.ContainsKey(targetEndpoint),
            TimeSpan.FromSeconds(5));
        Assert.True(rearmed, "expected InteractiveRestored for the killed node.");

        foreach (var e in otherEndpoints)
        {
            Assert.Equal(beforeOthers[e], _fx.Armer.RedirectTargets[e]);
        }

        // Prove tracking on the affected node works again, with a key that actually hashes there.
        var anyServer = mux.GetServer(endpoints[0]);
        var key = TestHelpers.KeyForEndPoint(mux, anyServer, targetEndpoint);
        await _fx.Cache.SetAsync(key, "v1");
        Assert.Equal("v1", await _fx.Cache.GetAsync<string>(key));
        Assert.True(_fx.Cache.TryGetLocal<string>(key, out _));

        RedisCli.Cluster(targetPort, "SET", key, "v2");
        var evicted = await Poll.UntilAsync(() => !_fx.Cache.TryGetLocal<string>(key, out _));
        Assert.True(evicted, "tracking on the re-armed node did not evict the key.");
    }
}
