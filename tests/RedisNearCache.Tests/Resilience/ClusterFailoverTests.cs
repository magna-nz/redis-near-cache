using System.Net;
using RedisNearCache.Tests.Chaos;
using RedisNearCache.Tests.EdgeCases;
using StackExchange.Redis;
using Xunit;
using Xunit.Abstractions;

namespace RedisNearCache.Tests.Resilience;

/// <summary>
/// A coordinated cluster failover: the replica of 7100 is promoted and 7100 becomes its replica. Unlike a node
/// restart, nothing crashes and no connection necessarily breaks - the slots simply belong to a different node
/// from one moment to the next. That is the dangerous shape for this library, because tracking is per node.
/// Since 0.5.2 the armer pre-arms replicas, so the promoted node already tracks every read routed to it
/// (<see cref="ClusterFailoverPreArmTests"/> covers that window); this test checks the bookkeeping that follows:
/// the promoted node ends up in <c>RedirectTargets</c>, the old master stops being treated as armed since it is a
/// replica now, and every node still delivers invalidations afterwards.
/// </summary>
/// <remarks>
/// Commands used (inside the <c>redis-near-cache-cluster</c> container):
/// <c>redis-cli -p &lt;replica&gt; CLUSTER FAILOVER</c> to promote, and the same against 7100 once it is a
/// replica to fail back. The <c>finally</c> restores masters 7100-7102 because the rest of the suite assumes
/// that layout.
/// </remarks>
public class ClusterFailoverTests
{
    private const int OldMasterPort = 7100;

    /// <summary>
    /// StackExchange.Redis follows the <c>MOVED</c> from the demoted master without raising
    /// <c>ConfigurationChanged</c>; what tells the armer about the promotion is the multiplexer's periodic
    /// topology check. The private multiplexer runs that check every 5 s (RedisNearCacheConnection sets
    /// <see cref="ConfigurationOptions.ConfigCheckSeconds"/>), so the promoted master must be armed well
    /// within this deadline. The test prints the measured delay. Before that setting existed it was ~60 s.
    /// </summary>
    private static readonly TimeSpan ArmDeadline = TimeSpan.FromSeconds(30);

    private readonly ITestOutputHelper _out;

    public ClusterFailoverTests(ITestOutputHelper output) => _out = output;

    [Fact]
    public async Task ClusterFailoverPromotesReplica()
    {
        Assert.True(ResilienceSupport.IsDefaultLayout(),
            "the cluster did not start from the default layout: " + ResilienceSupport.DescribeLayout());

        var before = ResilienceSupport.ClusterNodes();
        var oldMaster = ResilienceSupport.NodeOnPort(before, OldMasterPort);
        Assert.NotNull(oldMaster);
        var replica = before.FirstOrDefault(n => !n.IsMaster && n.MasterId == oldMaster!.Id);
        Assert.NotNull(replica);
        var newMasterPort = replica!.Port;
        _out.WriteLine($"failing over {OldMasterPort} -> {newMasterPort}; layout before: {ResilienceSupport.DescribeLayout()}");

        var handle = await EdgeCaseSupport.BuildAsync(ClusterCacheFixture.ConnectionString);
        ForeignClient? truth = null;
        var removed = new List<EndPoint>();
        try
        {
            var cache = handle.Cache;
            var mux = handle.Multiplexer;
            var masters = handle.Connection.ConnectedMasters().Select(s => s.EndPoint!).ToArray();
            Assert.Equal(3, masters.Length);

            var keyByEndpoint = TestHelpers.KeyPerMaster(mux, mux.GetServer(masters[0]), masters);
            Assert.Equal(3, keyByEndpoint.Count);
            var movedEndpoint = masters.Single(e => ResilienceSupport.PortOf(e) == OldMasterPort);
            var movedKey = keyByEndpoint[movedEndpoint];

            foreach (var (_, key) in keyByEndpoint)
            {
                await cache.SetAsync(key, "v1");
                Assert.True(await TestHelpers.ReadUntilCachedAsync(cache, key, "v1"), $"{key} was not cached before the failover.");
            }

            handle.Armer.EndpointRemoved += ep => { lock (removed) removed.Add(ep); };

            var failover = ResilienceSupport.ClusterFailover(newMasterPort);
            Assert.True(failover.ExitCode == 0 && failover.StdOut.Contains("OK", StringComparison.OrdinalIgnoreCase),
                $"CLUSTER FAILOVER on {newMasterPort} failed: {failover.StdOut} {failover.StdErr}");

            // The cluster's own view flips first.
            var flipped = await Poll.UntilAsync(
                () =>
                {
                    var nodes = ResilienceSupport.ClusterNodes(7101);
                    return ResilienceSupport.NodeOnPort(nodes, newMasterPort) is { IsMaster: true }
                           && ResilienceSupport.NodeOnPort(nodes, OldMasterPort) is { IsMaster: false };
                },
                TimeSpan.FromSeconds(30), TimeSpan.FromMilliseconds(200));
            Assert.True(flipped, "the cluster never promoted the replica: " + ResilienceSupport.DescribeLayout());
            _out.WriteLine($"layout after failover: {ResilienceSupport.DescribeLayout()}");

            // StackExchange.Redis only rediscovers the topology when something makes it: a read of a moved slot
            // comes back MOVED, which triggers the reconfiguration the armer listens for. The L1 copy has to be
            // dropped first or every read is a local hit and nothing ever reaches the server - which is itself
            // worth knowing: until some read misses (or ConfigCheckSeconds fires), a client can keep serving
            // entries whose slots now live on a node it has never armed.
            var armClock = System.Diagnostics.Stopwatch.StartNew();
            var armedNewMaster = await Poll.UntilAsync(
                async () =>
                {
                    cache.EvictLocal(movedKey);
                    try { await cache.GetAsync<string>(movedKey); }
                    catch (RedisException) { /* mid-reconfiguration */ }
                    return handle.Armer.RedirectTargets.Keys.Any(e => ResilienceSupport.PortOf(e) == newMasterPort);
                },
                ArmDeadline, TimeSpan.FromMilliseconds(250));
            armClock.Stop();

            var targets = string.Join(",", handle.Armer.RedirectTargets.Select(kv => $"{kv.Key}=>{kv.Value}"));
            _out.WriteLine($"promoted master armed after {armClock.ElapsedMilliseconds} ms; targets={targets}; " +
                           $"removed={string.Join(",", removed)}; stats={cache.Statistics}");
            Assert.True(armedNewMaster,
                $"the promoted master {newMasterPort} was never armed, so writes to slots 0-5460 produce no invalidation. targets={targets}");

            // The old master is a replica now. The armer must either have forgotten it (EndpointRemoved) or
            // still list it while it is connected - but it must not be treated as a master.
            var demoted = await Poll.UntilAsync(
                () => SafeIsReplica(mux, movedEndpoint) == true,
                TimeSpan.FromSeconds(30), TimeSpan.FromMilliseconds(200));
            Assert.True(demoted, $"the multiplexer still reports {OldMasterPort} as a master after the failover.");
            Assert.DoesNotContain(handle.Connection.ConnectedMasters().Select(s => ResilienceSupport.PortOf(s.EndPoint!)), p => p == OldMasterPort);

            // Reads of the moved slot are served correctly and cached again by the new owner.
            Assert.True(await TestHelpers.ReadUntilCachedAsync(cache, movedKey, "v1", TimeSpan.FromSeconds(30)),
                "the moved slot's key was not cached again after the failover.");
            var hitsBefore = cache.Statistics.Hits;
            Assert.Equal("v1", await cache.GetAsync<string>(movedKey));
            Assert.Equal(hitsBefore + 1, cache.Statistics.Hits);

            // And an external cluster write to that slot evicts, which is only possible if CLIENT TRACKING was
            // armed on the promoted node.
            RedisCli.Cluster(7101, "SET", movedKey, "v2");
            var evicted = await Poll.UntilAsync(() => !cache.TryGetLocal<string>(movedKey, out _), TimeSpan.FromSeconds(15));
            Assert.True(evicted,
                $"a write to a slot now owned by {newMasterPort} did not evict: invalidations from the promoted master are being lost.");
            Assert.Equal("v2", await ChaosSupport.WithReconnectRetryAsync(async () => await cache.GetAsync<string>(movedKey)));

            // The other two nodes were untouched and must still work.
            foreach (var (endpoint, key) in keyByEndpoint)
            {
                var port = ResilienceSupport.PortOf(endpoint);
                if (port == OldMasterPort) continue;
                Assert.True(await TestHelpers.ReadUntilCachedAsync(cache, key, "v1", TimeSpan.FromSeconds(20)), $"{key} was not cached after the failover.");
                RedisCli.Cluster(port, "SET", key, "v2");
                var otherEvicted = await Poll.UntilAsync(() => !cache.TryGetLocal<string>(key, out _), TimeSpan.FromSeconds(15));
                Assert.True(otherEvicted, $"node {port} stopped delivering invalidations after the failover of {OldMasterPort}.");
            }

            truth = await ForeignClient.ConnectAsync(ClusterCacheFixture.ConnectionString);
            Assert.True(await ChaosSupport.QuiesceAsync(cache, timeout: TimeSpan.FromSeconds(30)), "the cache never settled after the failover.");
            var stale = await ChaosSupport.FindStaleAsync(cache, truth, keyByEndpoint.Values);
            Assert.True(stale.Count == 0, "L1 held pre-failover values:\n" + string.Join("\n", stale));

            foreach (var (_, key) in keyByEndpoint) RedisCli.Cluster(7101, "DEL", key);
        }
        finally
        {
            if (truth is not null) await truth.DisposeAsync();
            await handle.DisposeAsync();

            var restored = await ResilienceSupport.RestoreDefaultMastersAsync();
            _out.WriteLine($"layout after fail-back: {ResilienceSupport.DescribeLayout()}");
            Assert.True(restored, "could not fail the cluster back to masters 7100-7102: " + ResilienceSupport.DescribeLayout());
            Assert.True(
                await Poll.UntilAsync(ResilienceSupport.IsDefaultLayout, TimeSpan.FromSeconds(60), TimeSpan.FromMilliseconds(250)),
                "the cluster did not return to the default slot layout: " + ResilienceSupport.DescribeLayout());
        }
    }

    private static bool? SafeIsReplica(IConnectionMultiplexer mux, EndPoint endPoint)
    {
        try
        {
            return mux.GetServer(endPoint).IsReplica;
        }
        catch (Exception ex) when (ex is RedisConnectionException or RedisTimeoutException or ArgumentException)
        {
            return null;
        }
    }
}
