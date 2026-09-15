using Xunit;

namespace RedisNearCache.Tests.Broadcast;

public class ArmedAtStartTests : IClassFixture<BroadcastStandaloneCacheFixture>
{
    private readonly BroadcastStandaloneCacheFixture _fx;

    public ArmedAtStartTests(BroadcastStandaloneCacheFixture fx) => _fx = fx;

    [Fact]
    public void ArmedStandaloneHasOneRedirectTargetAndNoPreArmedReplicas()
    {
        var endpoint = _fx.Connection.Multiplexer.GetEndPoints()[0];
        Assert.Single(_fx.Armer.RedirectTargets);
        Assert.Contains(endpoint, _fx.Armer.RedirectTargets.Keys);
        Assert.Empty(_fx.Armer.ReplicaRedirectTargets);
    }

    [Fact]
    public void BroadcastSocketAppearsInClientListUnderTheDashBcastName()
    {
        var expectedName = $"{_fx.Connection.ClientName}-bcast";
        var clientList = _fx.Server().ClientList();
        Assert.Contains(clientList, c => c.Name == expectedName);
    }
}

public class ClusterArmedAtStartTests : IClassFixture<BroadcastClusterCacheFixture>
{
    private readonly BroadcastClusterCacheFixture _fx;

    public ClusterArmedAtStartTests(BroadcastClusterCacheFixture fx) => _fx = fx;

    [Fact]
    public void ArmedClusterHasOneRedirectTargetPerMasterAndNoPreArmedReplicas()
    {
        Assert.Equal(BroadcastClusterCacheFixture.MasterPorts.Length, _fx.Armer.RedirectTargets.Count);
        Assert.Empty(_fx.Armer.ReplicaRedirectTargets);
    }
}
