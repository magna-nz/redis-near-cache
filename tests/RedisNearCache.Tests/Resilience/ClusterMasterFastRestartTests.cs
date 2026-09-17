using System.Net;
using RedisNearCache.Internal;
using RedisNearCache.Tests.Chaos;
using RedisNearCache.Tests.EdgeCases;
using Xunit;
using Xunit.Abstractions;

namespace RedisNearCache.Tests.Resilience;

/// <summary>
/// Redirect mode. A killed cluster master used to be forgotten - which lets the facade serve from L1 again - as soon
/// as <c>CLUSTER NODES</c> showed its slots had moved (the retry loop), or as soon as it restarted and a re-arm found
/// it a replica, without checking that the node now serving its slots was tracked. After a kill the promoted replica
/// is usually not pre-armed any more (a replica whose replication link is down is disarmed, and a connection failure
/// drops the pre-arm), and it is armed only once the multiplexer learns it is a master, which nothing bounds; writes
/// there meanwhile produce no invalidations. This test kills 7100, drops our connections to its replica 7103 so its
/// pre-arm is certainly gone, restarts 7100 as soon as 7103 is promoted, and pins the order: 7100 is forgotten only
/// once 7103 is tracked, and the cache never reports coherent after the kill while 7103 is not.
/// </summary>
/// <remarks>
/// Restores the default topology (7100-7102 masters, 7103-7105 their replicas) in a <c>finally</c>.
/// </remarks>
public class ClusterMasterFastRestartTests
{
    private const int KilledPort = 7100;
    private const int PromotedPort = 7103;
    private static readonly TimeSpan EventTimeout = TimeSpan.FromSeconds(60);

    private readonly ITestOutputHelper _out;

    public ClusterMasterFastRestartTests(ITestOutputHelper output) => _out = output;

    [Fact]
    public async Task RestartedMasterIsForgottenOnlyOncePromotedNodeIsTracked()
    {
        await ClusterNodes.RestoreDefaultTopologyAsync(_out.WriteLine);

        var handle = await EdgeCaseSupport.BuildAsync(ClusterCacheFixture.ConnectionString, captureLogs: true);
        var sw = System.Diagnostics.Stopwatch.StartNew();
        Exception? failure = null;
        var killed = false;
        try
        {
            var cache = handle.Cache;
            var mux = handle.Multiplexer;
            var armer = handle.Armer;
            Assert.Equal(3, armer.RedirectTargets.Count);
            var killedEndpoint = armer.RedirectTargets.Keys.First(e => ResilienceSupport.PortOf(e) == KilledPort);
            var survivorEndpoint = armer.RedirectTargets.Keys.First(e => !e.Equals(killedEndpoint));

            var key = TestHelpers.KeyForEndPoint(mux, mux.GetServer(survivorEndpoint), killedEndpoint);
            await cache.SetAsync(key, "v1");
            Assert.True(await TestHelpers.ReadUntilCachedAsync(cache, key, "v1"), $"{key} was not cached before the kill.");

            var gate = new object();
            var removed = new List<EndPoint>();
            var armed = new List<(int Port, ArmReason Reason, TimeSpan At)>();
            int[]? trackedAtRemoval = null;
            TimeSpan? lostAt = null;
            armer.TrackingLost += ep =>
            {
                lock (gate) if (ResilienceSupport.PortOf(ep) == KilledPort) lostAt ??= sw.Elapsed;
            };
            armer.EndpointRemoved += ep =>
            {
                var ports = TrackedPorts(armer);
                lock (gate)
                {
                    removed.Add(ep);
                    if (ResilienceSupport.PortOf(ep) == KilledPort) trackedAtRemoval ??= ports;
                }
            };
            armer.Armed += e => { lock (gate) armed.Add((ResilienceSupport.PortOf(e.EndPoint), e.Reason, sw.Elapsed)); };

            // Every moment the cache reported coherent, which nodes were tracked (armed masters and pre-armed replicas,
            // read on both sides of the coherence read and united).
            using var sampling = new CancellationTokenSource();
            var coherentSamples = new List<(TimeSpan At, int[] TrackedPorts)>();
            var sampler = Task.Run(async () =>
            {
                while (!sampling.IsCancellationRequested)
                {
                    var at = sw.Elapsed;
                    var before = TrackedPorts(armer);
                    var coherentNow = cache.IsCoherent;
                    var after = TrackedPorts(armer);
                    if (coherentNow)
                    {
                        lock (gate) coherentSamples.Add((at, before.Union(after).ToArray()));
                    }
                    try { await Task.Delay(20, sampling.Token); } catch (Exception) { return; }
                }
            });

            killed = true;
            DockerExec.Run(RedisCli.ClusterContainer, "redis-cli", "-p", KilledPort.ToString(), "SHUTDOWN", "NOSAVE");
            Assert.True(ClusterNodes.SpinUntil(() => !ClusterNodes.PingOk(KilledPort), TimeSpan.FromSeconds(15), TimeSpan.FromMilliseconds(200)),
                $"node {KilledPort} did not shut down.");
            _out.WriteLine($"[{sw.Elapsed}] killed {KilledPort}");

            // Drop our connections to the replica: that forgets its pre-arm, and with its master down its replication
            // link is not up, so the pre-arm sweep cannot restore it before the promotion.
            var clientList = DockerExec.Run(RedisCli.ClusterContainer, "redis-cli", "-p", PromotedPort.ToString(), "CLIENT", "LIST").StdOut;
            foreach (var id in EdgeCaseSupport.ClientIdsNamed(clientList, handle.Connection.ClientName))
                DockerExec.Run(RedisCli.ClusterContainer, "redis-cli", "-p", PromotedPort.ToString(), "CLIENT", "KILL", "ID", id.ToString());
            Assert.True(await Poll.UntilAsync(() => !armer.ReplicaRedirectTargets.Keys.Any(e => ResilienceSupport.PortOf(e) == PromotedPort), TimeSpan.FromSeconds(10)),
                $"{PromotedPort} is still pre-armed after its connections were killed.");
            _out.WriteLine($"[{sw.Elapsed}] dropped our connections to {PromotedPort}; pre-armed replicas [{string.Join(",", armer.ReplicaRedirectTargets.Keys)}]");

            var promoted = await Poll.UntilAsync(() =>
                ResilienceSupport.NodeOnPort(ResilienceSupport.ClusterNodes(7101), PromotedPort) is { IsMaster: true },
                TimeSpan.FromSeconds(30), TimeSpan.FromMilliseconds(100));
            Assert.True(promoted, $"{PromotedPort} was never promoted: {string.Join(" ", ResilienceSupport.ClusterNodes(7101).Select(n => $"{n.Port}{(n.IsMaster ? "M" : "S")}"))}");
            _out.WriteLine($"[{sw.Elapsed}] {PromotedPort} promoted; restarting {KilledPort}");

            ClusterNodes.StartNode(KilledPort);
            Assert.True(ClusterNodes.SpinUntil(() => ClusterNodes.PingOk(KilledPort), TimeSpan.FromSeconds(30), TimeSpan.FromMilliseconds(100)),
                $"node {KilledPort} did not come back up.");
            _out.WriteLine($"[{sw.Elapsed}] {KilledPort} is back");

            var forgotten = await Poll.UntilAsync(() =>
            {
                lock (gate) return removed.Any(ep => ResilienceSupport.PortOf(ep) == KilledPort);
            }, EventTimeout);
            Assert.True(forgotten, $"{KilledPort} was never forgotten: [{string.Join(", ", armer.RedirectTargets.Keys)}]");

            int[] trackedWhenRemoved;
            string armedEvents;
            lock (gate)
            {
                trackedWhenRemoved = trackedAtRemoval!;
                armedEvents = string.Join(", ", armed.Select(a => $"{a.Port}:{a.Reason}@{a.At}"));
            }
            _out.WriteLine($"[{sw.Elapsed}] {KilledPort} forgotten with tracked [{string.Join(",", trackedWhenRemoved)}]; Armed events [{armedEvents}]");
            Assert.True(trackedWhenRemoved.Contains(PromotedPort),
                $"{KilledPort} was forgotten (letting the cache serve from L1 again) while {PromotedPort}, which now serves its slots, " +
                $"was not tracked: tracked then [{string.Join(",", trackedWhenRemoved)}]; Armed events [{armedEvents}]");

            var settled = await Poll.UntilAsync(
                () => armer.RedirectTargets.Count == 3
                      && armer.RedirectTargets.Keys.Any(e => ResilienceSupport.PortOf(e) == PromotedPort)
                      && !armer.RedirectTargets.Keys.Any(e => ResilienceSupport.PortOf(e) == KilledPort),
                TimeSpan.FromSeconds(30));
            Assert.True(settled, $"RedirectTargets did not settle to the three current masters: [{string.Join(", ", armer.RedirectTargets.Keys)}]");
            Assert.True(await Poll.UntilAsync(() => cache.IsCoherent, TimeSpan.FromSeconds(15)), "the cache never reported coherent after the failover.");

            await sampling.CancelAsync();
            await sampler;
            (TimeSpan At, int[] TrackedPorts)[] unsafeSamples;
            lock (gate) unsafeSamples = coherentSamples.Where(s => s.At > lostAt!.Value && !s.TrackedPorts.Contains(PromotedPort)).ToArray();
            Assert.True(unsafeSamples.Length == 0,
                $"the cache reported coherent after losing {KilledPort} while {PromotedPort} (serving its slots) was not tracked, " +
                $"first at {unsafeSamples.FirstOrDefault().At} with tracked [{string.Join(",", unsafeSamples.FirstOrDefault().TrackedPorts ?? [])}]");

            Assert.True(await TestHelpers.ReadUntilCachedAsync(cache, key, "v1", TimeSpan.FromSeconds(15)), $"{key} was not cached again after the failover.");
            RedisCli.Cluster(PromotedPort, "SET", key, "v2");
            var evicted = await Poll.UntilAsync(() => !cache.TryGetLocal<string>(key, out _), TimeSpan.FromSeconds(10));
            Assert.True(evicted, "a foreign write to the promoted master did not evict the key: its reads are not tracked.");
            _out.WriteLine($"[{sw.Elapsed}] all assertions passed");
        }
        catch (Exception ex)
        {
            failure = ex;
            foreach (var line in handle.LogLines.Where(l => l.Contains("Tracking", StringComparison.Ordinal))) _out.WriteLine(line);
            throw;
        }
        finally
        {
            await handle.DisposeAsync();
            try
            {
                if (killed && !ClusterNodes.PingOk(KilledPort)) ClusterNodes.StartNode(KilledPort);
                if (killed)
                {
                    ClusterNodes.SpinUntil(() => ClusterNodes.PingOk(KilledPort), TimeSpan.FromSeconds(30), TimeSpan.FromMilliseconds(200));
                    ClusterNodes.SpinUntil(() => ResilienceSupport.NodeOnPort(ResilienceSupport.ClusterNodes(7101), KilledPort) is { IsMaster: false },
                        TimeSpan.FromSeconds(30), TimeSpan.FromMilliseconds(200));
                }
                await ClusterNodes.RestoreDefaultTopologyAsync(_out.WriteLine);
            }
            catch (Exception ex) when (failure is not null)
            {
                _out.WriteLine($"cluster restoration also failed after the test failure: {ex}");
            }
        }
    }

    private static int[] TrackedPorts(ITrackingArmer armer) =>
        armer.RedirectTargets.Keys.Concat(armer.ReplicaRedirectTargets.Keys).Select(ResilienceSupport.PortOf).ToArray();
}
