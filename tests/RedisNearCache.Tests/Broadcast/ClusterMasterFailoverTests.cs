using System.Net;
using Microsoft.Extensions.DependencyInjection;
using RedisNearCache.Internal;
using RedisNearCache.Tests.Chaos;
using StackExchange.Redis;
using Xunit;
using Xunit.Abstractions;

namespace RedisNearCache.Tests.Broadcast;

/// <summary>
/// Regression test for a verifier-found blocker: before the fix, the Broadcast tracker's background retry loop
/// (<c>BroadcastTracker.RetryLater</c>) never consulted <c>CLUSTER NODES</c>, so a killed cluster master stayed
/// "lost" forever - <see cref="ITrackingArmer.EndpointRemoved"/> was never raised for it and the whole facade
/// stayed in pass-through for the process lifetime. This test forces a real cluster failover: it kills one of the
/// three masters the rest of the Broadcast suite shares and lets its replica actually take the slots over (unlike
/// <see cref="NodeRestartClusterTests"/>, which restarts the very same node quickly enough that no failover ever
/// happens), then proves the killed endpoint is forgotten once its slots move and the promoted replica is armed
/// with <see cref="ArmReason.TopologyChanged"/> at the next reconcile.
/// </summary>
/// <remarks>
/// Builds its own provider per test instead of sharing <see cref="BroadcastClusterCacheFixture"/>, because this
/// test deliberately breaks the cluster container the rest of the suite shares. The cluster is restored to its
/// original topology (7100-7102 masters, 7103-7105 replicas) in a <c>finally</c>, the same way
/// <see cref="NodeRestartClusterTests"/> restarts a node, plus a <c>CLUSTER FAILOVER</c> to hand the killed
/// port's mastership back, so later tests in the same run see the cluster exactly as they expect it.
/// </remarks>
public class ClusterMasterFailoverTests
{
    private const int TargetMasterPort = 7100;
    private static readonly TimeSpan EventTimeout = TimeSpan.FromSeconds(60);

    private readonly ITestOutputHelper _out;

    public ClusterMasterFailoverTests(ITestOutputHelper output) => _out = output;

    [Fact]
    public async Task KilledClusterMasterIsForgottenAndReplicaIsArmed()
    {
        var services = new ServiceCollection();
        services.AddRedisNearCache(BroadcastClusterCacheFixture.ConnectionString, o =>
        {
            o.TrackingMode = TrackingMode.Broadcast;
            o.KeyPrefixes.Add(BroadcastKey.Prefix);
        });
        var provider = services.BuildServiceProvider();
        var sw = System.Diagnostics.Stopwatch.StartNew();

        Exception? failure = null;
        try
        {
            var cache = provider.GetRequiredService<IRedisNearCache>();
            var connection = provider.GetRequiredService<RedisNearCacheConnection>();
            var armer = provider.GetRequiredService<ITrackingArmer>();
            await cache.Ready;

            var mux = connection.Multiplexer;
            Assert.Equal(3, armer.RedirectTargets.Count);

            var targetEndpoint = armer.RedirectTargets.Keys.First(e => ((IPEndPoint)e).Port == TargetMasterPort);
            // Query from a master that stays up the whole test, never the one about to be killed.
            var survivorEndpoint = armer.RedirectTargets.Keys.First(e => !e.Equals(targetEndpoint));
            var survivorServer = mux.GetServer(survivorEndpoint);

            var topology = survivorServer.ClusterNodes();
            Assert.NotNull(topology);
            var targetNode = topology![targetEndpoint];
            Assert.False(targetNode is null || targetNode.IsReplica, $"{targetEndpoint} was not reported as a master before the kill.");
            var replicaNode = targetNode!.Children.FirstOrDefault();
            Assert.NotNull(replicaNode);
            var replicaEndpoint = replicaNode!.EndPoint!;
            var replicaPort = ((IPEndPoint)replicaEndpoint).Port;
            _out.WriteLine($"target master port {TargetMasterPort}; its replica (expected promotion target) is port {replicaPort}");

            // A key that actually hashes to the master we are about to kill, under the prefix the broadcast
            // socket is armed for, so a foreign write to it after the failover is what proves invalidations
            // resumed on the promoted shard.
            var key = KeyForEndPointUnderPrefix(mux, survivorServer, targetEndpoint);
            await cache.SetAsync(key, "v1");
            Assert.True(await TestHelpers.ReadUntilCachedAsync(cache, key, "v1"), $"{key} was not cached before the kill.");

            var gate = new object();
            var trackingLost = new List<EndPoint>();
            var endpointRemoved = new List<EndPoint>();
            var armed = new List<(EndPoint EndPoint, ArmReason Reason, TimeSpan At)>();
            armer.TrackingLost += ep => { lock (gate) trackingLost.Add(ep); };
            armer.EndpointRemoved += ep => { lock (gate) endpointRemoved.Add(ep); };
            armer.Armed += e => { lock (gate) armed.Add((e.EndPoint, e.Reason, sw.Elapsed)); };

            KillNode(TargetMasterPort);
            _out.WriteLine($"[{sw.Elapsed}] killed node {TargetMasterPort} (SHUTDOWN NOSAVE, left down for the cluster to fail it over for real)");

            var lostSeen = await Poll.UntilAsync(() =>
            {
                lock (gate) return trackingLost.Any(ep => ((IPEndPoint)ep).Port == TargetMasterPort);
            }, EventTimeout);
            Assert.True(lostSeen, "TrackingLost was not raised for the killed master.");
            _out.WriteLine($"[{sw.Elapsed}] TrackingLost observed for {TargetMasterPort}");

            var removed = await Poll.UntilAsync(() =>
            {
                lock (gate) return endpointRemoved.Any(ep => ((IPEndPoint)ep).Port == TargetMasterPort);
            }, EventTimeout);
            Assert.True(removed,
                "EndpointRemoved was never raised for the killed master: it stayed a 'known master' forever. " +
                "This is exactly the blocker the fix (RetryLater consulting MasterRole.IsKnownMasterAsync / CLUSTER NODES) addresses.");
            _out.WriteLine($"[{sw.Elapsed}] EndpointRemoved observed for {TargetMasterPort}");

            var promoted = await Poll.UntilAsync(() =>
            {
                lock (gate) return armed.Any(a => a.Reason == ArmReason.TopologyChanged && ((IPEndPoint)a.EndPoint).Port == replicaPort);
            }, EventTimeout);
            Assert.True(promoted, $"expected an Armed event with reason TopologyChanged for the promoted replica {replicaPort}.");
            TimeSpan armedAt;
            lock (gate)
            {
                armedAt = armed.First(a => a.Reason == ArmReason.TopologyChanged && ((IPEndPoint)a.EndPoint).Port == replicaPort).At;
            }
            _out.WriteLine($"[{armedAt}] Armed(TopologyChanged) observed for promoted replica {replicaPort}");

            var settled = await Poll.UntilAsync(
                () => armer.RedirectTargets.Count == 3 && !armer.RedirectTargets.Keys.Any(e => ((IPEndPoint)e).Port == TargetMasterPort),
                TimeSpan.FromSeconds(15));
            Assert.True(settled,
                $"RedirectTargets did not settle back to 3 entries without {TargetMasterPort}: [{string.Join(", ", armer.RedirectTargets.Keys)}]");

            var coherent = await Poll.UntilAsync(() => cache.IsCoherent, TimeSpan.FromSeconds(15));
            Assert.True(coherent, "the cache never reported coherent again after the failover.");

            Assert.True(await TestHelpers.ReadUntilCachedAsync(cache, key, "v1", TimeSpan.FromSeconds(15)),
                $"{key} was not cached again on the surviving shard after the failover.");

            RedisCli.Cluster(replicaPort, "SET", key, "v2");
            var evicted = await Poll.UntilAsync(() => !cache.TryGetLocal<string>(key, out _), TimeSpan.FromSeconds(10));
            Assert.True(evicted, "a foreign write to the promoted shard did not evict the key: invalidations from it are not being received.");
            _out.WriteLine($"[{sw.Elapsed}] all assertions passed");
        }
        catch (Exception ex)
        {
            failure = ex;
            throw;
        }
        finally
        {
            await provider.DisposeAsync();
            try
            {
                RestoreCluster(TargetMasterPort, _out);
            }
            catch (Exception ex) when (failure is not null)
            {
                // The test already failed; a restoration failure must not replace that as the reported error.
                _out.WriteLine($"cluster restoration also failed after the test failure: {ex}");
            }
        }
    }

    /// <summary>
    /// Generates keys under <see cref="BroadcastKey.Prefix"/> until one hashes to the given endpoint's slot range.
    /// Same idea as <see cref="TestHelpers.KeyForEndPoint"/>, but under the prefix the broadcast socket in this
    /// suite is armed for, so a write to the key is actually broadcast.
    /// </summary>
    private static string KeyForEndPointUnderPrefix(IConnectionMultiplexer mux, IServer anyServer, EndPoint endpoint)
    {
        for (var i = 0; i < 20_000; i++)
        {
            var candidate = BroadcastKey.New($"node{i}");
            var slot = mux.GetHashSlot(candidate);
            var node = anyServer.ClusterNodes()?.GetBySlot(slot);
            if (node?.EndPoint?.Equals(endpoint) == true) return candidate;
        }

        throw new InvalidOperationException($"could not find a key hashing to node {endpoint}.");
    }

    /// <summary>Kills one cluster node's redis-server, the same way <see cref="NodeRestartClusterTests"/> does, and waits until it stops responding.</summary>
    private static void KillNode(int port)
    {
        // SHUTDOWN legitimately drops the connection mid-command, so a non-zero exit here is not a failure.
        DockerExec.Run(RedisCli.ClusterContainer, "redis-cli", "-p", port.ToString(), "SHUTDOWN", "NOSAVE");

        var down = ClusterNodes.SpinUntil(() => !ClusterNodes.PingOk(port), TimeSpan.FromSeconds(15), TimeSpan.FromMilliseconds(200));
        Assert.True(down, $"node {port} did not shut down.");
    }

    /// <summary>
    /// Puts the killed master back exactly as the suite found it. A restarted node whose slots were taken over
    /// while it was down rejoins the cluster's gossip and turns itself into a replica of whoever now owns them
    /// automatically (verified by hand against this same container before writing this test); once it is a
    /// caught-up replica, <c>CLUSTER FAILOVER</c> run on it hands mastership straight back, so ports 7100-7102 are
    /// masters again for the rest of the Broadcast suite.
    /// </summary>
    private static void RestoreCluster(int port, ITestOutputHelper output)
    {
        ClusterNodes.StartNode(port);

        var up = ClusterNodes.SpinUntil(() => ClusterNodes.PingOk(port), TimeSpan.FromSeconds(30), TimeSpan.FromMilliseconds(200));
        Assert.True(up, $"node {port} did not come back up.");

        var isReplica = ClusterNodes.SpinUntil(() => RoleIsConnectedReplica(port), TimeSpan.FromSeconds(30), TimeSpan.FromMilliseconds(200));
        Assert.True(isReplica, $"node {port} never rejoined as a connected replica of its new master.");

        var failover = DockerExec.Run(RedisCli.ClusterContainer, "redis-cli", "-p", port.ToString(), "CLUSTER", "FAILOVER");
        Assert.True(failover.ExitCode == 0 && failover.StdOut.Contains("OK", StringComparison.Ordinal),
            $"CLUSTER FAILOVER on {port} did not report OK: {failover.StdOut} {failover.StdErr}");

        var isMasterAgain = ClusterNodes.SpinUntil(() => RoleIsMaster(port), TimeSpan.FromSeconds(30), TimeSpan.FromMilliseconds(200));
        Assert.True(isMasterAgain, $"node {port} did not become a master again after CLUSTER FAILOVER.");

        var clusterOk = ClusterNodes.SpinUntil(() => BroadcastClusterCacheFixture.MasterPorts.All(ClusterNodes.ClusterStateOk), TimeSpan.FromSeconds(30), TimeSpan.FromMilliseconds(200));
        Assert.True(clusterOk, "the cluster never returned to cluster_state:ok after restoring the killed master.");

        var info = DockerExec.Run(RedisCli.ClusterContainer, "redis-cli", "-p", port.ToString(), "CLUSTER", "INFO");
        output.WriteLine($"final CLUSTER INFO on {port}: {info.StdOut}");
        Assert.Contains("cluster_state:ok", info.StdOut, StringComparison.Ordinal);
    }

    /// <summary><c>ROLE</c>'s first line is "slave" once the node is a replica, and its fourth line is "connected" once its link to the new master is up (not "connect"/"connecting"/"sync" while it is still catching up).</summary>
    private static bool RoleIsConnectedReplica(int port)
    {
        var lines = RoleLines(port);
        return lines is { Length: >= 4 } && lines[0] == "slave" && lines[3] == "connected";
    }

    private static bool RoleIsMaster(int port)
    {
        var lines = RoleLines(port);
        return lines is { Length: >= 1 } && lines[0] == "master";
    }

    private static string[]? RoleLines(int port)
    {
        var r = DockerExec.Run(RedisCli.ClusterContainer, "redis-cli", "-p", port.ToString(), "ROLE");
        if (r.ExitCode != 0) return null;
        return r.StdOut.Split('\n', StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries);
    }
}
