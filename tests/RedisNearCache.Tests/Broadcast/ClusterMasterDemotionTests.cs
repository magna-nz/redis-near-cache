using System.Net;
using Microsoft.Extensions.DependencyInjection;
using RedisNearCache.Internal;
using RedisNearCache.Tests.Resilience;
using StackExchange.Redis;
using Xunit;
using Xunit.Abstractions;

namespace RedisNearCache.Tests.Broadcast;

/// <summary>
/// The reconcile path of <c>BroadcastTracker</c> used to forget any endpoint the multiplexer reports as a replica at
/// once. When a master is demoted while connected (a manual <c>CLUSTER FAILOVER</c>, or a killed master that restarts
/// and rejoins as a replica), the multiplexer can learn the demotion in the same reconfigure that reveals the promoted
/// node, so the old master was forgotten - letting the facade serve from L1 again - before the promoted node had an
/// armed socket, and writes there produced no invalidations. This test demotes 7100 with <c>CLUSTER FAILOVER</c> on its
/// replica, forces the private multiplexer to reconfigure (standing in for its periodic check, which then sees both
/// role changes at once), and pins the order: 7100 is forgotten only once the promoted node is armed, and the cache
/// never reports coherent while neither of them has an armed socket.
/// </summary>
/// <remarks>
/// Builds its own provider, like <see cref="ClusterMasterFailoverTests"/>, and restores the default topology
/// (7100-7102 masters, 7103-7105 their replicas) in a <c>finally</c>.
/// </remarks>
public class ClusterMasterDemotionTests
{
    private const int DemotedPort = 7100;
    private const int PromotedPort = 7103;
    private static readonly TimeSpan EventTimeout = TimeSpan.FromSeconds(60);

    private readonly ITestOutputHelper _out;

    public ClusterMasterDemotionTests(ITestOutputHelper output) => _out = output;

    [Fact]
    public async Task DemotedMasterIsForgottenOnlyOncePromotedNodeIsArmed()
    {
        await ClusterNodes.RestoreDefaultTopologyAsync(_out.WriteLine);

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
            var demotedEndpoint = armer.RedirectTargets.Keys.First(e => ResilienceSupport.PortOf(e) == DemotedPort);
            var survivorEndpoint = armer.RedirectTargets.Keys.First(e => !e.Equals(demotedEndpoint));

            var key = KeyForEndPointUnderPrefix(mux, mux.GetServer(survivorEndpoint), demotedEndpoint);
            await cache.SetAsync(key, "v1");
            Assert.True(await TestHelpers.ReadUntilCachedAsync(cache, key, "v1"), $"{key} was not cached before the failover.");

            var gate = new object();
            var removed = new List<EndPoint>();
            var armed = new List<(int Port, ArmReason Reason, TimeSpan At)>();
            int[]? armedAtRemoval = null;
            armer.EndpointRemoved += ep =>
            {
                var ports = armer.RedirectTargets.Keys.Select(ResilienceSupport.PortOf).ToArray();
                lock (gate)
                {
                    removed.Add(ep);
                    if (ResilienceSupport.PortOf(ep) == DemotedPort) armedAtRemoval ??= ports;
                }
            };
            armer.Armed += e => { lock (gate) armed.Add((ResilienceSupport.PortOf(e.EndPoint), e.Reason, sw.Elapsed)); };

            // Every moment the cache reported coherent, which sockets were armed (read on both sides of the coherence
            // read and united, so a sample only lacks a port that was unarmed throughout).
            using var sampling = new CancellationTokenSource();
            var coherentSamples = new List<(TimeSpan At, int[] ArmedPorts)>();
            var sampler = Task.Run(async () =>
            {
                while (!sampling.IsCancellationRequested)
                {
                    var at = sw.Elapsed;
                    var portsBefore = armer.RedirectTargets.Keys.Select(ResilienceSupport.PortOf).ToArray();
                    var coherentNow = cache.IsCoherent;
                    var portsAfter = armer.RedirectTargets.Keys.Select(ResilienceSupport.PortOf).ToArray();
                    if (coherentNow)
                    {
                        lock (gate) coherentSamples.Add((at, portsBefore.Union(portsAfter).ToArray()));
                    }
                    try { await Task.Delay(20, sampling.Token); } catch (Exception) { return; }
                }
            });

            var failoverAt = sw.Elapsed;
            var flipped = await ResilienceSupport.FailoverUntilPromotedAsync(PromotedPort, DemotedPort, TimeSpan.FromSeconds(40), _out.WriteLine);
            Assert.True(flipped, "the cluster never promoted the replica: " + ResilienceSupport.DescribeLayout());
            _out.WriteLine($"[{sw.Elapsed}] cluster flipped: {ResilienceSupport.DescribeLayout()}");

            // The multiplexer's periodic check, forced: it now sees 7100 as a replica and 7103 as a master in one go.
            await mux.ConfigureAsync();
            _out.WriteLine($"[{sw.Elapsed}] private multiplexer reconfigured");

            var forgotten = await Poll.UntilAsync(() =>
            {
                lock (gate) return removed.Any(ep => ResilienceSupport.PortOf(ep) == DemotedPort);
            }, EventTimeout);
            Assert.True(forgotten, $"the demoted master {DemotedPort} was never forgotten: [{string.Join(", ", armer.RedirectTargets.Keys)}]");

            int[] armedWhenRemoved;
            string armedEvents;
            lock (gate)
            {
                armedWhenRemoved = armedAtRemoval!;
                armedEvents = string.Join(", ", armed.Select(a => $"{a.Port}:{a.Reason}@{a.At}"));
            }
            _out.WriteLine($"[{sw.Elapsed}] {DemotedPort} forgotten with armed [{string.Join(",", armedWhenRemoved)}]; Armed events [{armedEvents}]");
            Assert.True(armedWhenRemoved.Contains(PromotedPort),
                $"{DemotedPort} was forgotten (letting the cache serve from L1 again) while {PromotedPort}, which now serves its slots, " +
                $"had no armed socket: armed then [{string.Join(",", armedWhenRemoved)}]; Armed events [{armedEvents}]");

            var settled = await Poll.UntilAsync(
                () => armer.RedirectTargets.Count == 3
                      && armer.RedirectTargets.Keys.Any(e => ResilienceSupport.PortOf(e) == PromotedPort)
                      && !armer.RedirectTargets.Keys.Any(e => ResilienceSupport.PortOf(e) == DemotedPort),
                TimeSpan.FromSeconds(15));
            Assert.True(settled, $"RedirectTargets did not settle to the three current masters: [{string.Join(", ", armer.RedirectTargets.Keys)}]");
            Assert.True(await Poll.UntilAsync(() => cache.IsCoherent, TimeSpan.FromSeconds(15)), "the cache never reported coherent after the failover.");

            await sampling.CancelAsync();
            await sampler;
            (TimeSpan At, int[] ArmedPorts)[] unsafeSamples;
            lock (gate)
            {
                unsafeSamples = coherentSamples
                    .Where(s => s.At > failoverAt && !s.ArmedPorts.Contains(DemotedPort) && !s.ArmedPorts.Contains(PromotedPort))
                    .ToArray();
            }
            Assert.True(unsafeSamples.Length == 0,
                $"the cache reported coherent while neither {DemotedPort} nor {PromotedPort} had an armed socket, " +
                $"first at {unsafeSamples.FirstOrDefault().At} with armed [{string.Join(",", unsafeSamples.FirstOrDefault().ArmedPorts ?? [])}]");

            Assert.True(await TestHelpers.ReadUntilCachedAsync(cache, key, "v1", TimeSpan.FromSeconds(15)),
                $"{key} was not cached again after the failover.");
            RedisCli.Cluster(PromotedPort, "SET", key, "v2");
            var evicted = await Poll.UntilAsync(() => !cache.TryGetLocal<string>(key, out _), TimeSpan.FromSeconds(10));
            Assert.True(evicted, "a foreign write to the promoted master did not evict the key: invalidations from it are not being received.");
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
                await ClusterNodes.RestoreDefaultTopologyAsync(_out.WriteLine);
            }
            catch (Exception ex) when (failure is not null)
            {
                _out.WriteLine($"cluster restoration also failed after the test failure: {ex}");
            }
        }
    }

    /// <summary>A key under <see cref="BroadcastKey.Prefix"/> that hashes to <paramref name="endpoint"/>'s slots, so a write to it is broadcast.</summary>
    private static string KeyForEndPointUnderPrefix(IConnectionMultiplexer mux, IServer anyServer, EndPoint endpoint)
    {
        var nodes = anyServer.ClusterNodes();
        for (var i = 0; i < 20_000; i++)
        {
            var candidate = BroadcastKey.New($"node{i}");
            if (nodes?.GetBySlot(mux.GetHashSlot(candidate))?.EndPoint?.Equals(endpoint) == true) return candidate;
        }

        throw new InvalidOperationException($"could not find a key hashing to node {endpoint}.");
    }
}
