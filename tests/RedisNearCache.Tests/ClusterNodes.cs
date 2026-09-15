using System.Globalization;
using RedisNearCache.Tests.Chaos;
using Xunit;

namespace RedisNearCache.Tests;

/// <summary>
/// Shared low-level control over the cluster container's <c>redis-server</c> processes, used by tests that kill
/// and restart a cluster node directly (as opposed to just its connections): a generic poll loop, liveness/state
/// checks via <c>redis-cli</c>, and the exact <c>redis-server</c> argv <c>cluster-up.sh</c> starts each node
/// with, so a restarted node rejoins with its old node id and re-reads its old cluster config file. Used by
/// <see cref="RedisNearCache.Tests.Chaos.NodeRestartClusterTests"/> and
/// <see cref="RedisNearCache.Tests.Broadcast.ClusterMasterFailoverTests"/>, which both restart cluster nodes but
/// for different reasons (a quick restart vs. a kill left down long enough to force a real failover).
/// </summary>
internal static class ClusterNodes
{
    /// <summary>Polls <paramref name="condition"/> until it is true or <paramref name="timeout"/> elapses, sleeping <paramref name="pollInterval"/> (default 100ms) between checks.</summary>
    public static bool SpinUntil(Func<bool> condition, TimeSpan timeout, TimeSpan? pollInterval = null)
    {
        var interval = pollInterval ?? TimeSpan.FromMilliseconds(100);
        var deadline = DateTime.UtcNow + timeout;
        while (DateTime.UtcNow < deadline)
        {
            if (condition()) return true;
            Thread.Sleep(interval);
        }

        return condition();
    }

    public static bool PingOk(int port)
    {
        var r = DockerExec.Run(RedisCli.ClusterContainer, "redis-cli", "-p", port.ToString(), "PING");
        return r.ExitCode == 0 && r.StdOut.Contains("PONG", StringComparison.Ordinal);
    }

    public static bool ClusterStateOk(int port)
    {
        var r = DockerExec.Run(RedisCli.ClusterContainer, "redis-cli", "-p", port.ToString(), "CLUSTER", "INFO");
        return r.ExitCode == 0 && r.StdOut.Contains("cluster_state:ok", StringComparison.Ordinal);
    }

    /// <summary>Starts a fresh redis-server with exactly the argv <c>cluster-up.sh</c> used for this port, so it rejoins with its old node id and re-reads its old cluster config file.</summary>
    public static void StartNode(int port)
    {
        var started = DockerExec.Run(
            RedisCli.ClusterContainer,
            "redis-server",
            "--port", port.ToString(),
            "--cluster-enabled", "yes",
            "--cluster-config-file", $"nodes-{port}.conf",
            "--cluster-announce-ip", "127.0.0.1",
            "--cluster-announce-port", port.ToString(),
            "--cluster-announce-bus-port", (port + 10_000).ToString(),
            "--save", "",
            "--appendonly", "no",
            "--daemonize", "yes");

        Assert.True(started.ExitCode == 0, $"could not restart node {port}: {started.StdErr} {started.StdOut}");
    }

    /// <summary>The replica of each default master, as <c>cluster-up.sh</c> pairs them.</summary>
    public static readonly (int Master, int Replica)[] DefaultPairs = [(7100, 7103), (7101, 7104), (7102, 7105)];

    /// <summary>
    /// Puts the cluster back exactly as <c>cluster-up.sh</c> left it: 7100-7102 masters owning their default
    /// slot ranges, and 7103-7105 replicating them in that order. A failover test that only waits for its own
    /// node to be a master again leaves the demoted node briefly as an empty master, or attached to a different
    /// master, and the next test to read <c>CLUSTER NODES</c> then finds no (or the wrong) replica.
    /// </summary>
    public static async Task RestoreDefaultTopologyAsync(Action<string>? log = null)
    {
        var mastersBack = await Resilience.ResilienceSupport.RestoreDefaultMastersAsync().ConfigureAwait(false);
        log?.Invoke($"layout after fail-back: {Resilience.ResilienceSupport.DescribeLayout()}");
        Assert.True(mastersBack, "could not fail the cluster back to masters 7100-7102: " + Resilience.ResilienceSupport.DescribeLayout());
        Assert.True(
            await Poll.UntilAsync(Resilience.ResilienceSupport.IsDefaultLayout, TimeSpan.FromSeconds(60), TimeSpan.FromMilliseconds(250)).ConfigureAwait(false),
            "the cluster did not return to the default slot layout: " + Resilience.ResilienceSupport.DescribeLayout());

        // Re-pair every replica with its original master. CLUSTER REPLICATE is idempotent, so it is sent whenever the
        // pairing is not already right, and the poll below waits for gossip to agree.
        foreach (var (master, replica) in DefaultPairs)
        {
            var nodes = Resilience.ResilienceSupport.ClusterNodes();
            var masterNode = Resilience.ResilienceSupport.NodeOnPort(nodes, master);
            var replicaNode = Resilience.ResilienceSupport.NodeOnPort(nodes, replica);
            if (masterNode is null || replicaNode is null) continue;
            if (!replicaNode.IsMaster && replicaNode.MasterId == masterNode.Id) continue;
            var r = DockerExec.Run(RedisCli.ClusterContainer, "redis-cli", "-p", replica.ToString(CultureInfo.InvariantCulture), "CLUSTER", "REPLICATE", masterNode.Id);
            log?.Invoke($"CLUSTER REPLICATE {master} on {replica}: {r.StdOut.Trim()} {r.StdErr.Trim()}");
        }

        Assert.True(
            await Poll.UntilAsync(() =>
            {
                var nodes = Resilience.ResilienceSupport.ClusterNodes();
                return DefaultPairs.All(pair =>
                    Resilience.ResilienceSupport.NodeOnPort(nodes, pair.Master) is { IsMaster: true } m
                    && Resilience.ResilienceSupport.NodeOnPort(nodes, pair.Replica) is { IsMaster: false } rep
                    && rep.MasterId == m.Id)
                    && DefaultPairs.All(pair => ClusterStateOk(pair.Master));
            }, TimeSpan.FromSeconds(60), TimeSpan.FromMilliseconds(250)).ConfigureAwait(false),
            "replicas did not re-attach to their original masters: " + Resilience.ResilienceSupport.DescribeLayout());
    }
}
