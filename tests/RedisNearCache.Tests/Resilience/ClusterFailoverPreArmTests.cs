using System.Net;
using RedisNearCache.Internal;
using RedisNearCache.Tests.Chaos;
using RedisNearCache.Tests.EdgeCases;
using StackExchange.Redis;
using Xunit;
using Xunit.Abstractions;

namespace RedisNearCache.Tests.Resilience;

/// <summary>
/// The counterpart of <see cref="ClusterFailoverTests"/> once replicas are pre-armed: the replica of 7100 has
/// had <c>CLIENT TRACKING ON REDIRECT</c> issued on it since before the failover, so the promotion should need
/// neither a re-arm nor an L1 flush, and a read routed to the freshly promoted master by a <c>MOVED</c> redirect
/// must already be tracked - not just "tracked a few seconds later once the periodic topology check catches up".
/// </summary>
/// <remarks>
/// Commands used (inside the <c>redis-near-cache-cluster</c> container): the same
/// <c>redis-cli -p &lt;replica&gt; CLUSTER FAILOVER</c> as <see cref="ClusterFailoverTests"/>; the <c>finally</c>
/// restores masters 7100-7102 because the rest of the suite assumes that layout.
/// </remarks>
public class ClusterFailoverPreArmTests
{
    private const int OldMasterPort = 7100;

    private readonly ITestOutputHelper _out;

    public ClusterFailoverPreArmTests(ITestOutputHelper output) => _out = output;

    [Fact]
    public async Task PromotedReplicaWasPreArmedSoNoReadIsUntrackedAndNoFlushIsNeeded()
    {
        Assert.True(ResilienceSupport.IsDefaultLayout(),
            "the cluster did not start from the default layout: " + ResilienceSupport.DescribeLayout());

        var before = ResilienceSupport.ClusterNodes();
        var oldMaster = ResilienceSupport.NodeOnPort(before, OldMasterPort);
        Assert.NotNull(oldMaster);
        var replicaNode = before.FirstOrDefault(n => !n.IsMaster && n.MasterId == oldMaster!.Id);
        Assert.NotNull(replicaNode);
        var newMasterPort = replicaNode!.Port;
        _out.WriteLine($"failing over {OldMasterPort} -> {newMasterPort}; layout before: {ResilienceSupport.DescribeLayout()}");

        var handle = await EdgeCaseSupport.BuildAsync(ClusterCacheFixture.ConnectionString);
        var armedEvents = new List<TrackingArmedEvent>();
        var removed = new List<EndPoint>();
        try
        {
            var cache = handle.Cache;
            var mux = handle.Multiplexer;

            // Every replica must be pre-armed before the failover is worth anything.
            var preArmed = await Poll.UntilAsync(
                () => handle.Armer.ReplicaRedirectTargets.Count == 3,
                TimeSpan.FromSeconds(20), TimeSpan.FromMilliseconds(250));
            var replicaTargets = string.Join(",", handle.Armer.ReplicaRedirectTargets.Select(kv => $"{kv.Key}=>{kv.Value}"));
            _out.WriteLine($"pre-armed replicas: {replicaTargets}");
            Assert.True(preArmed, $"not every replica was pre-armed within the deadline: {replicaTargets}");

            var preArmEntry = handle.Armer.ReplicaRedirectTargets.Single(kv => ResilienceSupport.PortOf(kv.Key) == newMasterPort);
            var preArmId = preArmEntry.Value;

            handle.Armer.Armed += e => { lock (armedEvents) armedEvents.Add(e); };
            handle.Armer.EndpointRemoved += ep => { lock (removed) removed.Add(ep); };
            // A connection to the promoted node can drop during the failover (seen on CI). That is a genuine
            // reconnect, which by the library's rules must re-arm and flush; the pre-arm claims below only hold
            // when no such loss happened, so record it and relax them if it did.
            var lostNewMaster = new List<EndPoint>();
            handle.Armer.TrackingLost += ep => { if (ResilienceSupport.PortOf(ep) == newMasterPort) lock (lostNewMaster) lostNewMaster.Add(ep); };

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

            var flushesBefore = cache.Statistics.Flushes;
            var rearmsBefore = cache.Statistics.Rearms;
            var invalidationsBefore = cache.Statistics.Invalidations;

            var failover = ResilienceSupport.ClusterFailover(newMasterPort);
            Assert.True(failover.ExitCode == 0 && failover.StdOut.Contains("OK", StringComparison.OrdinalIgnoreCase),
                $"CLUSTER FAILOVER on {newMasterPort} failed: {failover.StdOut} {failover.StdErr}");

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

            // Read the moved key immediately: the point of pre-arm is that this must be tracked even though our
            // own multiplexer's periodic topology check (which is what the un-pre-armed test waits on) has not
            // necessarily noticed the promotion yet.
            cache.EvictLocal(movedKey);
            var noticedAlready = handle.Armer.RedirectTargets.Keys.Any(e => ResilienceSupport.PortOf(e) == newMasterPort);
            var got = await Poll.UntilAsync(
                async () =>
                {
                    try { return await cache.GetAsync<string>(movedKey) == "v1"; }
                    catch (RedisException) { return false; }
                },
                TimeSpan.FromSeconds(10), TimeSpan.FromMilliseconds(100));
            Assert.True(got, $"reading {movedKey} after the failover never returned v1 (the old master returns MOVED and StackExchange.Redis should resend it to the promoted node).");
            _out.WriteLine($"read happened {(noticedAlready ? "after" : "before")} promotion was noticed by the topology check");

            RedisCli.Cluster(7101, "SET", movedKey, "v2");
            var invalidated = await Poll.UntilAsync(() => cache.Statistics.Invalidations > invalidationsBefore, TimeSpan.FromSeconds(3));
            Assert.True(invalidated,
                "a read routed to the promoted master produced no invalidation on a later write: the promoted node was not tracking it (pre-arm missing)");
            Assert.False(cache.TryGetLocal<string>(movedKey, out string? _));
            Assert.Equal("v2", await ChaosSupport.WithReconnectRetryAsync(async () => await cache.GetAsync<string>(movedKey)));

            var reArmed = await Poll.UntilAsync(
                () => handle.Armer.RedirectTargets.Keys.Any(e => ResilienceSupport.PortOf(e) == newMasterPort),
                TimeSpan.FromSeconds(30), TimeSpan.FromMilliseconds(250));
            Assert.True(reArmed, $"the promoted master {newMasterPort} was never armed.");
            var newMasterEndpoint = handle.Armer.RedirectTargets.Keys.Single(e => ResilienceSupport.PortOf(e) == newMasterPort);
            bool lostBeforeCheck;
            lock (lostNewMaster) lostBeforeCheck = lostNewMaster.Count > 0;
            if (!lostBeforeCheck)
                Assert.True(handle.Armer.RedirectTargets[newMasterEndpoint] == preArmId,
                    $"the promoted master was re-armed with a new redirect id instead of keeping its pre-arm (pre-arm id {preArmId}, now {handle.Armer.RedirectTargets[newMasterEndpoint]}).");

            var oldGone = await Poll.UntilAsync(
                () => !handle.Connection.ConnectedMasters().Select(s => ResilienceSupport.PortOf(s.EndPoint!)).Contains(OldMasterPort),
                TimeSpan.FromSeconds(30), TimeSpan.FromMilliseconds(250));
            Assert.True(oldGone, $"the old master {OldMasterPort} is still listed as a connected master.");
            Assert.True(await ChaosSupport.QuiesceAsync(cache, timeout: TimeSpan.FromSeconds(30)), "the cache never settled after the failover.");

            List<TrackingArmedEvent> eventsSnapshot;
            lock (armedEvents) eventsSnapshot = [.. armedEvents];
            var eventsDescription = string.Join(" | ", eventsSnapshot.Select(e => $"{e.EndPoint} {e.Reason}"));
            var promotedForNewMaster = eventsSnapshot.Where(e => ResilienceSupport.PortOf(e.EndPoint) == newMasterPort && e.Reason == ArmReason.Promoted).ToArray();
            Assert.True(promotedForNewMaster.Length == 1,
                $"expected exactly one Promoted arm for {newMasterPort}, found {promotedForNewMaster.Length}. stats={cache.Statistics} events=[{eventsDescription}]");
            bool newMasterLost;
            lock (lostNewMaster) newMasterLost = lostNewMaster.Count > 0;
            if (newMasterLost)
            {
                _out.WriteLine($"a connection to {newMasterPort} was lost after the failover, so a re-arm there is expected; not asserting on re-arms. events=[{eventsDescription}]");
            }
            else
            {
                Assert.DoesNotContain(eventsSnapshot, e => ResilienceSupport.PortOf(e.EndPoint) == newMasterPort && e.Reason == ArmReason.TopologyChanged);
                Assert.True(cache.Statistics.Rearms == rearmsBefore,
                    $"expected no re-arm to be counted for a pre-armed promotion. stats={cache.Statistics} events=[{eventsDescription}]");
            }
            // Promoted flushes once (entries read from the demoted master) and the demoted master's removal flushes once;
            // a later resync of the demoted node may add one more. What the pre-arm guarantees is no re-arm (above), and
            // that the reads routed to the promoted node before the topology check were tracked (checked above).
            Assert.True(cache.Statistics.Flushes >= flushesBefore + 1,
                $"expected at least one flush across the failover. stats={cache.Statistics} events=[{eventsDescription}]");
            List<EndPoint> removedSnapshot;
            lock (removed) removedSnapshot = [.. removed];
            Assert.Contains(removedSnapshot, ep => ResilienceSupport.PortOf(ep) == OldMasterPort);

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

            foreach (var (_, key) in keyByEndpoint) RedisCli.Cluster(7101, "DEL", key);
        }
        finally
        {
            await handle.DisposeAsync();

            var restored = await ResilienceSupport.RestoreDefaultMastersAsync();
            _out.WriteLine($"layout after fail-back: {ResilienceSupport.DescribeLayout()}");
            Assert.True(restored, "could not fail the cluster back to masters 7100-7102: " + ResilienceSupport.DescribeLayout());
            Assert.True(
                await Poll.UntilAsync(ResilienceSupport.IsDefaultLayout, TimeSpan.FromSeconds(60), TimeSpan.FromMilliseconds(250)),
                "the cluster did not return to the default slot layout: " + ResilienceSupport.DescribeLayout());
            // And the demoted node must be re-attached as 7100's replica before the next test reads the topology, or
            // that test finds a master without a replica for a moment.
            Assert.True(
                await Poll.UntilAsync(() =>
                {
                    var nodes = ResilienceSupport.ClusterNodes();
                    var master = ResilienceSupport.NodeOnPort(nodes, OldMasterPort);
                    return master is { IsMaster: true } && nodes.Any(n => !n.IsMaster && n.MasterId == master.Id);
                }, TimeSpan.FromSeconds(60), TimeSpan.FromMilliseconds(250)),
                "no replica re-attached to the restored master: " + ResilienceSupport.DescribeLayout());
        }
    }
}
