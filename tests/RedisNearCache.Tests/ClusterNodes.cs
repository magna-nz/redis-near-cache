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
}
