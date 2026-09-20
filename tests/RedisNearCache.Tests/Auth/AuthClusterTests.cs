using System.Net;
using RedisNearCache.Tests.EdgeCases;
using RedisNearCache.Tests.Resilience;
using StackExchange.Redis;
using Xunit;
using Xunit.Abstractions;

namespace RedisNearCache.Tests.Auth;

/// <summary>
/// The password-protected cluster <c>redis-near-cache-auth-cluster</c> (masters <c>127.0.0.1:7300-7302</c>,
/// replicas 7303-7305, <c>requirepass</c>/<c>masterauth</c> plus the three ACL users on every node). A cluster
/// is where an authentication gap that a single node hides shows up: the private multiplexer connects to six
/// nodes, arms three of them separately, pre-arms the replicas (Redirect mode), and StackExchange.Redis reads
/// its tie-breaker key and subscribes to its configuration channel on every one of them. Each test therefore
/// proves the end-to-end behaviour for a key on EACH master, not just for one.
/// </summary>
public class AuthClusterTests
{
    private readonly ITestOutputHelper _out;

    public AuthClusterTests(ITestOutputHelper output) => _out = output;

    /// <summary>A foreign write, following <c>MOVED</c> (<c>redis-cli -c</c>) so it lands on the owning master.</summary>
    private static void ExternalSet(string key, string value) => AuthSupport.Cluster("SET", key, value);

    /// <summary>
    /// One key per master, found the way <see cref="TestHelpers.KeyForEndPoint"/> does: hash the candidate and ask
    /// <c>CLUSTER NODES</c> which node owns its slot. The prefix is fixed by the caller because the minimal ACL
    /// users can only touch <c>t:</c> / <c>bc:</c> keys.
    /// </summary>
    private static Dictionary<int, string> KeyPerMaster(EdgeCaseProvider handle, string prefix)
    {
        var mux = handle.Multiplexer;
        var anyServer = mux.GetServer(mux.GetEndPoints().First());
        var result = new Dictionary<int, string>();
        for (var i = 0; i < 40_000 && result.Count < AuthSupport.ClusterMasterPorts.Length; i++)
        {
            var candidate = $"{prefix}{Guid.NewGuid():N}:n{i}";
            var node = anyServer.ClusterNodes()?.GetBySlot(mux.GetHashSlot(candidate));
            if (node?.EndPoint is not { } endpoint) continue;
            var port = ResilienceSupport.PortOf(endpoint);
            if (AuthSupport.ClusterMasterPorts.Contains(port) && !result.ContainsKey(port)) result[port] = candidate;
        }

        Assert.Equal(AuthSupport.ClusterMasterPorts.Length, result.Count);
        return result;
    }

    /// <summary>Every master of the deployment is armed, and a key on each of them behaves end to end.</summary>
    private async Task AssertEveryMasterWorksAsync(EdgeCaseProvider handle, string prefix, string context)
    {
        var armedPorts = handle.Armer.RedirectTargets.Keys
            .Select(ResilienceSupport.PortOf)
            .Where(AuthSupport.ClusterMasterPorts.Contains)
            .Distinct()
            .OrderBy(p => p)
            .ToArray();
        Assert.Equal(AuthSupport.ClusterMasterPorts.OrderBy(p => p).ToArray(), armedPorts);

        var keys = KeyPerMaster(handle, prefix);
        try
        {
            foreach (var (port, key) in keys.OrderBy(kv => kv.Key))
            {
                await AuthSupport.AssertCacheWorksAsync(handle.Cache, key, ExternalSet, $"{context}, master {port}");
                _out.WriteLine($"{context}: master {port} ok via {key}");
            }
        }
        finally
        {
            foreach (var key in keys.Values)
            {
                try { AuthSupport.Cluster("DEL", key); }
                catch (InvalidOperationException) { /* best effort cleanup */ }
            }
        }
    }

    /// <summary>
    /// <c>password=</c> only, Redirect mode: every master armed and invalidating, with the password carried to all
    /// six nodes the multiplexer discovers (the connection string names three).
    /// </summary>
    [Fact]
    public async Task Cluster_PasswordOnly_Redirect_Works()
    {
        var handle = await EdgeCaseSupport.BuildAsync(AuthSupport.PasswordOnly(AuthSupport.ClusterEndPoints));
        try
        {
            await AssertEveryMasterWorksAsync(handle, AuthSupport.InsidePrefix, "cluster password-only redirect");
        }
        finally
        {
            await handle.DisposeAsync();
        }
    }

    /// <summary><c>rnc-full</c> (<c>~* &amp;* +@all</c>) in Redirect mode across all three masters.</summary>
    [Fact]
    public async Task Cluster_FullAclUser_Redirect_Works()
    {
        var handle = await EdgeCaseSupport.BuildAsync(
            AuthSupport.AsUser(AuthSupport.ClusterEndPoints, AuthSupport.FullUser, AuthSupport.FullPassword));
        try
        {
            await AssertEveryMasterWorksAsync(handle, AuthSupport.InsidePrefix, "cluster rnc-full redirect");
        }
        finally
        {
            await handle.DisposeAsync();
        }
    }

    /// <summary>
    /// <c>rnc-minimal-redirect</c> across all three masters. This is where a missing StackExchange.Redis grant shows
    /// up that a single node hides: the tie-breaker <c>GET __Booksleeve_TieBreak</c> and the
    /// <c>__Booksleeve_MasterChanged</c> subscription happen per node, <c>CLUSTER NODES</c> is read for routing and
    /// by the library's own reconcile, and <c>ROLE</c> is sent to each of the three replicas to pre-arm them.
    /// Asserts nothing was denied on any node.
    /// </summary>
    [Fact]
    public async Task Cluster_MinimalAclUser_Redirect_Works_WithNothingDenied()
    {
        foreach (var port in AllPorts) AuthSupport.ResetAclLog(args => AuthSupport.ClusterNode(port, args));
        var handle = await EdgeCaseSupport.BuildAsync(
            AuthSupport.AsUser(AuthSupport.ClusterEndPoints, AuthSupport.MinimalRedirectUser, AuthSupport.MinimalPassword),
            o => o.KeyPrefixes.Add(AuthSupport.InsidePrefix),
            captureLogs: true);
        try
        {
            await AssertEveryMasterWorksAsync(handle, AuthSupport.InsidePrefix, "cluster rnc-minimal-redirect");
            AssertNothingDeniedAnywhere(handle, AuthSupport.MinimalRedirectUser, "cluster rnc-minimal-redirect");
        }
        finally
        {
            await handle.DisposeAsync();
        }
    }

    /// <summary>
    /// <c>rnc-minimal-bcast</c> in Broadcast mode across all three masters: one hand-rolled RESP3 socket per master,
    /// each authenticating with <c>HELLO 3 AUTH rnc-minimal-bcast rnc-minimal-pw</c> and arming
    /// <c>CLIENT TRACKING ON BCAST PREFIX bc:</c>. Asserts nothing was denied on any node.
    /// </summary>
    [Fact]
    public async Task Cluster_MinimalAclUser_Broadcast_Works_WithNothingDenied()
    {
        foreach (var port in AllPorts) AuthSupport.ResetAclLog(args => AuthSupport.ClusterNode(port, args));
        var handle = await EdgeCaseSupport.BuildAsync(
            AuthSupport.AsUser(AuthSupport.ClusterEndPoints, AuthSupport.MinimalBroadcastUser, AuthSupport.MinimalPassword),
            o =>
            {
                o.TrackingMode = TrackingMode.Broadcast;
                o.KeyPrefixes.Add(AuthSupport.BroadcastPrefix);
            },
            captureLogs: true);
        try
        {
            await AssertEveryMasterWorksAsync(handle, AuthSupport.BroadcastPrefix, "cluster rnc-minimal-bcast");
            AssertNothingDeniedAnywhere(handle, AuthSupport.MinimalBroadcastUser, "cluster rnc-minimal-bcast");
        }
        finally
        {
            await handle.DisposeAsync();
        }
    }

    /// <summary>Masters and replicas: a denial on a replica (the pre-arm's <c>ROLE</c>) counts just as much.</summary>
    private static readonly int[] AllPorts = [7300, 7301, 7302, 7303, 7304, 7305];

    private static void AssertNothingDeniedAnywhere(EdgeCaseProvider handle, string user, string context)
    {
        // Masters only: that is where the multiplexer's connections and the broadcast sockets are guaranteed to be.
        foreach (var port in AuthSupport.ClusterMasterPorts)
        {
            AuthSupport.AssertConnectedAs(
                args => AuthSupport.ClusterNode(port, args), handle.Connection.ClientName, user, $"{context} on node {port}");
        }

        foreach (var port in AllPorts)
        {
            AuthSupport.AssertNothingDenied(
                AuthSupport.AclLog(args => AuthSupport.ClusterNode(port, args)),
                handle.LogLines,
                $"{context} on node {port}");
        }
    }
}
