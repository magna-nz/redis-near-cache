using System.Net;
using StackExchange.Redis;
using Xunit;
using Xunit.Abstractions;

namespace RedisNearCache.Tests.Chaos;

/// <summary>
/// Losing a whole cluster node, not just a connection. A restarted node comes back with its client id
/// counter reset, so the redirect id the armer recorded before the restart is meaningless afterwards; if the
/// re-arm does not re-discover it, every invalidation from that node is lost silently while the other two
/// nodes keep working — the hardest kind of incoherence to notice in production.
/// </summary>
/// <remarks>
/// The task for this test called for <c>DEBUG RESTART</c>. That is not available: the <c>redis:7.4</c> image
/// runs with <c>enable-debug-command</c> unset, which rejects every DEBUG subcommand, and the setting is
/// immutable so <c>CONFIG SET</c> cannot turn it on. The node is therefore restarted the equivalent way:
/// <c>SHUTDOWN NOSAVE</c>, then a fresh <c>redis-server</c> with exactly the argv from <c>cluster-up.sh</c>,
/// which re-reads <c>nodes-7101.conf</c> and rejoins the cluster owning the same slots.
/// </remarks>
public class NodeRestartClusterTests : IClassFixture<ClusterCacheFixture>
{
    private const int RestartPort = 7101;

    private readonly ClusterCacheFixture _fx;
    private readonly ITestOutputHelper _out;

    public NodeRestartClusterTests(ClusterCacheFixture fx, ITestOutputHelper output)
    {
        _fx = fx;
        _out = output;
    }

    [Fact]
    public async Task NodeRestartCluster()
    {
        var mux = _fx.Connection.Multiplexer;
        var endpoints = mux.GetEndPoints();
        var anyServer = mux.GetServer(endpoints[0]);
        var target = endpoints.First(e => ((IPEndPoint)e).Port == RestartPort);

        var keyByEndpoint = TestHelpers.KeyPerMaster(mux, anyServer, endpoints);
        Assert.Equal(3, keyByEndpoint.Count);

        // One key cached per master before the restart.
        foreach (var (_, key) in keyByEndpoint)
        {
            await _fx.Cache.SetAsync(key, "v1");
            Assert.Equal("v1", await _fx.Cache.GetAsync<string>(key));
            Assert.True(_fx.Cache.TryGetLocal<string>(key, out _));
        }

        var redirectBefore = _fx.Armer.RedirectTargets[target];

        RestartNode(RestartPort);

        var clusterBack = await Poll.UntilAsync(
            () => ClusterCacheFixture.MasterPorts.All(ClusterStateOk),
            TimeSpan.FromSeconds(30), TimeSpan.FromMilliseconds(200));
        Assert.True(clusterBack, "the cluster never returned to cluster_state:ok after the node restart.");

        var reconnected = await Poll.UntilAsync(
            () => SafeIsConnected(mux, target), TimeSpan.FromSeconds(30));
        Assert.True(reconnected, "the private multiplexer never reported the restarted endpoint as connected.");

        // Re-armed means: the recorded redirect target is a client id that exists on the RESTARTED node right
        // now and is our subscriber there. A stale id from before the restart would fail this even if it
        // happened to compare equal to the new one by luck.
        var rearmed = await Poll.UntilAsync(
            () => _fx.Armer.RedirectTargets.TryGetValue(target, out var current)
                  && LiveSubscriberId(mux, target) is { } live
                  && current == live,
            TimeSpan.FromSeconds(30));

        _out.WriteLine($"redirect for {target}: {redirectBefore} -> " +
                       $"{(_fx.Armer.RedirectTargets.TryGetValue(target, out var after) ? after : -1)}; " +
                       $"live subscriber id there = {LiveSubscriberId(mux, target)}; stats={_fx.Cache.Statistics}");

        Assert.True(rearmed, "tracking was not re-armed at a live subscriber id on the restarted node.");

        // The restart emptied that node, and losing a connection flushes L1 everywhere, so re-seed and
        // re-read all three keys before testing eviction.
        foreach (var (_, key) in keyByEndpoint)
        {
            await ChaosSupport.WithReconnectRetryAsync(async () => await _fx.Cache.SetAsync(key, "v1"), TimeSpan.FromSeconds(30));
            Assert.True(await TestHelpers.ReadUntilCachedAsync(_fx.Cache, key, "v1", TimeSpan.FromSeconds(30)), $"{key} was not cached after the restart.");
        }

        // The restarted node first, then the other two: all three must still deliver invalidations.
        var restartedKey = keyByEndpoint[target];
        RedisCli.Cluster(RestartPort, "SET", restartedKey, "v2");
        var restartedEvicted = await Poll.UntilAsync(
            () => !_fx.Cache.TryGetLocal<string>(restartedKey, out _), TimeSpan.FromSeconds(10));
        Assert.True(restartedEvicted, "the restarted node's invalidations are being lost: its key is still in L1.");

        foreach (var (endpoint, key) in keyByEndpoint)
        {
            if (endpoint.Equals(target)) continue;
            var port = ((IPEndPoint)endpoint).Port;
            RedisCli.Cluster(port, "SET", key, "v2");
            var evicted = await Poll.UntilAsync(() => !_fx.Cache.TryGetLocal<string>(key, out _), TimeSpan.FromSeconds(10));
            Assert.True(evicted, $"node {port} stopped delivering invalidations after node {RestartPort} was restarted.");
        }
    }

    /// <summary>Kills one node's redis-server and starts a replacement with the argv from cluster-up.sh.</summary>
    private static void RestartNode(int port)
    {
        // SHUTDOWN legitimately drops the connection mid-command, so a non-zero exit here is not a failure.
        DockerExec.Run(RedisCli.ClusterContainer, "redis-cli", "-p", port.ToString(), "SHUTDOWN", "NOSAVE");

        var down = SpinUntil(() => !PingOk(port), TimeSpan.FromSeconds(15));
        Assert.True(down, $"node {port} did not shut down.");

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

    private static bool SpinUntil(Func<bool> condition, TimeSpan timeout)
    {
        var deadline = DateTime.UtcNow + timeout;
        while (DateTime.UtcNow < deadline)
        {
            if (condition()) return true;
            Thread.Sleep(100);
        }
        return condition();
    }

    private static bool PingOk(int port)
    {
        var r = DockerExec.Run(RedisCli.ClusterContainer, "redis-cli", "-p", port.ToString(), "PING");
        return r.ExitCode == 0 && r.StdOut.Contains("PONG", StringComparison.Ordinal);
    }

    private static bool ClusterStateOk(int port)
    {
        var r = DockerExec.Run(RedisCli.ClusterContainer, "redis-cli", "-p", port.ToString(), "CLUSTER", "INFO");
        return r.ExitCode == 0 && r.StdOut.Contains("cluster_state:ok", StringComparison.Ordinal);
    }

    private static bool SafeIsConnected(IConnectionMultiplexer mux, EndPoint endpoint)
    {
        try
        {
            return mux.GetServer(endpoint).IsConnected;
        }
        catch (Exception ex) when (ex is RedisConnectionException or RedisTimeoutException)
        {
            return false;
        }
    }

    /// <summary>Our subscriber connection's client id on one node, as the node itself reports it right now.</summary>
    private long? LiveSubscriberId(IConnectionMultiplexer mux, EndPoint endpoint)
    {
        try
        {
            var server = mux.GetServer(endpoint);
            if (!server.IsConnected) return null;
            return server.ClientList()
                .Where(c => c.Name == _fx.Connection.ClientName && (c.Flags & ClientFlags.PubSubSubscriber) != 0)
                .Select(c => (long?)c.Id)
                .Max();
        }
        catch (Exception ex) when (ex is RedisConnectionException or RedisTimeoutException or RedisServerException)
        {
            return null;
        }
    }
}
