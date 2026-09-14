using System.Net;
using RedisNearCache.Tracking;
using StackExchange.Redis;
using Xunit;

namespace RedisNearCache.UnitTests;

/// <summary>
/// The "is this endpoint still a master of the deployment" decision that decides whether a lost endpoint keeps the
/// cache in pass-through (and is retried) or is forgotten. A killed node's role never changes in the multiplexer's
/// view, so the rules must look at what replaced it.
/// </summary>
public class MasterRoleTests
{
    private static readonly EndPoint A = new IPEndPoint(IPAddress.Loopback, 6401);
    private static readonly EndPoint B = new IPEndPoint(IPAddress.Loopback, 6402);
    private static readonly EndPoint C = new IPEndPoint(IPAddress.Loopback, 6400);

    private static ServerView S(EndPoint ep, bool connected, bool replica, ServerType type = ServerType.Standalone) => new(ep, connected, replica, type);

    // --- multiplexer view (Sentinel / standalone with replicas) -------------------------------------------------

    [Fact]
    public void KilledSentinelMasterWithPromotedReplicaIsNoLongerAMaster()
    {
        // The state from the hard-kill Sentinel test: 6401(down)M, 6402M, 6400R.
        var servers = new[] { S(A, connected: false, replica: false), S(B, connected: true, replica: false), S(C, connected: true, replica: true) };
        Assert.False(MasterRole.FromMultiplexer(A, servers));
        Assert.True(MasterRole.FromMultiplexer(B, servers));
    }

    [Fact]
    public void DownMasterWithoutReplacementStaysAMaster()
    {
        // Before Sentinel promotes anything: stay in pass-through and keep retrying, that is the safe behaviour.
        var servers = new[] { S(A, connected: false, replica: false), S(B, connected: true, replica: true), S(C, connected: true, replica: true) };
        Assert.True(MasterRole.FromMultiplexer(A, servers));
    }

    [Fact]
    public void DownMasterWhoseOnlyOtherMasterIsAlsoDownStaysAMaster()
    {
        var servers = new[] { S(A, connected: false, replica: false), S(B, connected: false, replica: false) };
        Assert.True(MasterRole.FromMultiplexer(A, servers));
    }

    [Fact]
    public void SentinelServersDoNotCountAsAReplacementMaster()
    {
        var sentinel = new IPEndPoint(IPAddress.Loopback, 26379);
        var servers = new[] { S(A, connected: false, replica: false), S(sentinel, connected: true, replica: false, ServerType.Sentinel) };
        Assert.True(MasterRole.FromMultiplexer(A, servers));
    }

    [Fact]
    public void ConnectedMasterIsAMasterEvenIfAnotherMasterExists()
    {
        // Mid graceful failover both nodes briefly say master; the old one is not dropped while it is reachable.
        var servers = new[] { S(A, connected: true, replica: false), S(B, connected: true, replica: false) };
        Assert.True(MasterRole.FromMultiplexer(A, servers));
    }

    [Fact]
    public void ReplicaIsNotAMasterConnectedOrNot()
    {
        Assert.False(MasterRole.FromMultiplexer(A, [S(A, connected: true, replica: true), S(B, connected: true, replica: false)]));
        Assert.False(MasterRole.FromMultiplexer(A, [S(A, connected: false, replica: true)]));
    }

    [Fact]
    public void EndpointTheMultiplexerNoLongerListsIsNotAMaster()
    {
        Assert.False(MasterRole.FromMultiplexer(A, [S(B, connected: true, replica: false)]));
    }

    [Fact]
    public void DisconnectedClusterMasterNeedsTheSlotMap()
    {
        var servers = new[] { S(A, connected: false, replica: false, ServerType.Cluster), S(B, connected: true, replica: false, ServerType.Cluster) };
        Assert.Null(MasterRole.FromMultiplexer(A, servers));
        Assert.True(MasterRole.FromMultiplexer(B, servers));
    }

    // --- cluster view ---------------------------------------------------------------------------------------------

    private static readonly EndPoint Node7100 = new IPEndPoint(IPAddress.Loopback, 7100);

    [Fact]
    public void DownClusterMasterThatStillOwnsSlotsStaysAMaster()
    {
        var nodes = new[]
        {
            new ClusterNodeView(Node7100, null, IsReplica: false, OwnsSlots: true),    // master,fail? - no failover yet
            new ClusterNodeView(new IPEndPoint(IPAddress.Loopback, 7103), null, IsReplica: true, OwnsSlots: false),
        };
        Assert.True(MasterRole.FromClusterNodes(Node7100, nodes));
    }

    [Fact]
    public void FailedOverClusterMasterWithoutSlotsIsNoLongerAMaster()
    {
        var nodes = new[]
        {
            new ClusterNodeView(Node7100, null, IsReplica: false, OwnsSlots: false),   // master,fail, slots moved away
            new ClusterNodeView(new IPEndPoint(IPAddress.Loopback, 7103), null, IsReplica: false, OwnsSlots: true),
        };
        Assert.False(MasterRole.FromClusterNodes(Node7100, nodes));
    }

    [Fact]
    public void ClusterNodeTheClusterForgotIsNoLongerAMaster()
    {
        var nodes = new[] { new ClusterNodeView(new IPEndPoint(IPAddress.Loopback, 7101), null, IsReplica: false, OwnsSlots: true) };
        Assert.False(MasterRole.FromClusterNodes(Node7100, nodes));
    }

    [Fact]
    public void NoClusterViewAtAllStaysAMaster()
    {
        Assert.True(MasterRole.FromClusterNodes(Node7100, null));
        Assert.True(MasterRole.FromClusterNodes(Node7100, []));
    }

    [Fact]
    public void HostnameAnnouncingMasterIsMatchedByAnnouncedHostnameNotEndPointEquality()
    {
        // Verified against the hostname cluster: GetServers() yields DnsEndPoint(localhost:7201) while CLUSTER NODES
        // parses 127.0.0.1:7201 with hostname "localhost". EndPoint equality would judge this healthy master slotless.
        var dns = new DnsEndPoint("localhost", 7201);
        var nodes = new[]
        {
            new ClusterNodeView(new IPEndPoint(IPAddress.Loopback, 7200), "localhost", IsReplica: false, OwnsSlots: true),
            new ClusterNodeView(new IPEndPoint(IPAddress.Loopback, 7201), "localhost", IsReplica: false, OwnsSlots: true),
        };
        Assert.False(dns.Equals(nodes[1].EndPoint));
        Assert.True(MasterRole.FromClusterNodes(dns, nodes));
        Assert.False(MasterRole.FromClusterNodes(new DnsEndPoint("localhost", 7202), nodes));
    }

    [Fact]
    public void DnsEndPointMatchesThroughResolvedAddressesWhenNoHostnameIsAnnounced()
    {
        var dns = new DnsEndPoint("redis-node-1.internal", 7100);
        var nodes = new[] { new ClusterNodeView(new IPEndPoint(IPAddress.Parse("10.0.0.5"), 7100), null, IsReplica: false, OwnsSlots: true) };
        Assert.False(MasterRole.FromClusterNodes(dns, nodes));
        Assert.True(MasterRole.FromClusterNodes(dns, nodes, [IPAddress.Parse("10.0.0.5")]));
    }

    [Fact]
    public void MatchingNormalizesIpv4MappedAddressesAndRequiresThePort()
    {
        var mapped = new IPEndPoint(IPAddress.Loopback.MapToIPv6(), 7100);
        Assert.True(MasterRole.Matches(mapped, new ClusterNodeView(Node7100, null, false, true)));
        Assert.False(MasterRole.Matches(new IPEndPoint(IPAddress.Loopback, 7101), new ClusterNodeView(Node7100, null, false, true)));
        Assert.False(MasterRole.Matches(Node7100, new ClusterNodeView(null, null, false, true)));
    }
}
