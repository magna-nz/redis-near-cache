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
/// with <see cref="ArmReason.TopologyChanged"/> at the next reconcile. It also pins the order of the two: the killed
/// endpoint is forgotten (and the facade caches again) only once the node now serving its slots is armed, never in
/// between, when a write there would produce no invalidation.
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
        // Earlier cluster tests fail back masters and re-pair replicas, and gossip about that settles after they
        // return: start from the known layout (7100's only replica is 7103), not from whatever is converging.
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
        var killed = false;
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
            var survivorPort = ((IPEndPoint)survivorEndpoint).Port;

            // Only a sanity check: which replica wins the election is Redis's call, so the promoted node is read
            // from the slot map after the failover rather than predicted here.
            var before = Resilience.ResilienceSupport.ClusterNodes(survivorPort);
            var targetId = Resilience.ResilienceSupport.NodeOnPort(before, TargetMasterPort)?.Id;
            var replicasOfTarget = before.Where(n => !n.IsMaster && n.MasterId == targetId).Select(n => n.Port).ToArray();
            Assert.True(targetId is not null && replicasOfTarget.Length > 0,
                $"{TargetMasterPort} has no replica to fail over to: {Resilience.ResilienceSupport.DescribeLayout()}");
            _out.WriteLine($"target master port {TargetMasterPort}; replicas that may be promoted: {string.Join(",", replicasOfTarget)}");

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
            // Which masters had an armed socket at the moment the killed one was forgotten: forgetting it is what lets
            // the facade cache again, so the node now serving its slots must already be among them.
            int[]? armedAtRemoval = null;
            TimeSpan? lostAt = null;
            armer.TrackingLost += ep =>
            {
                lock (gate)
                {
                    trackingLost.Add(ep);
                    if (((IPEndPoint)ep).Port == TargetMasterPort) lostAt ??= sw.Elapsed;
                }
            };
            armer.EndpointRemoved += ep =>
            {
                var ports = armer.RedirectTargets.Keys.Select(e => ((IPEndPoint)e).Port).ToArray();
                lock (gate)
                {
                    endpointRemoved.Add(ep);
                    if (((IPEndPoint)ep).Port == TargetMasterPort) armedAtRemoval ??= ports;
                }
            };
            armer.Armed += e => { lock (gate) armed.Add((e.EndPoint, e.Reason, sw.Elapsed)); };

            // The same property from the facade's side, sampled for the whole failover: every moment the cache
            // reported itself coherent, which armed sockets existed.
            using var sampling = new CancellationTokenSource();
            var coherentSamples = new List<(TimeSpan At, int[] ArmedPorts)>();
            var sampler = Task.Run(async () =>
            {
                while (!sampling.IsCancellationRequested)
                {
                    // Armed sockets read on both sides of the coherence read, and united: a socket installed or dropped
                    // in between shows up in one of them, so a sample only lacks a port that was unarmed throughout.
                    var at = sw.Elapsed;
                    var portsBefore = armer.RedirectTargets.Keys.Select(e => ((IPEndPoint)e).Port).ToArray();
                    var coherentNow = cache.IsCoherent;
                    var portsAfter = armer.RedirectTargets.Keys.Select(e => ((IPEndPoint)e).Port).ToArray();
                    if (coherentNow)
                    {
                        var ports = portsBefore.Union(portsAfter).ToArray();
                        lock (gate) coherentSamples.Add((at, ports));
                    }
                    try { await Task.Delay(20, sampling.Token); } catch (Exception) { return; } // cancelled, or disposed when the test failed early
                }
            });

            killed = true; // set first: a kill that fails half-way still needs the restart below
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

            // The slots have moved by now (that is what retired 7100): whoever serves the key's slot is the promoted node.
            var slot = mux.GetHashSlot(key);
            var replicaPort = 0;
            var ownerKnown = await Poll.UntilAsync(() =>
            {
                var owner = Resilience.ResilienceSupport.ClusterNodes(survivorPort)
                    .FirstOrDefault(n => n.IsMaster && n.Slots.Any(s => s.From <= slot && slot <= s.To));
                replicaPort = owner?.Port ?? 0;
                return owner is not null && owner.Port != TargetMasterPort;
            }, TimeSpan.FromSeconds(15));
            Assert.True(ownerKnown, $"no node other than {TargetMasterPort} serves slot {slot}: {Resilience.ResilienceSupport.DescribeLayout()}");
            Assert.Contains(replicaPort, replicasOfTarget);
            _out.WriteLine($"[{sw.Elapsed}] slot {slot} is now served by {replicaPort}");

            var promoted = await Poll.UntilAsync(() =>
            {
                lock (gate) return armed.Any(a => a.Reason == ArmReason.TopologyChanged && ((IPEndPoint)a.EndPoint).Port == replicaPort);
            }, EventTimeout);
            if (!promoted)
            {
                string seen;
                lock (gate) seen = string.Join(", ", armed.Select(a => $"{a.EndPoint}:{a.Reason}@{a.At}"));
                var views = string.Join(", ", mux.GetServers().Select(s => $"{s.EndPoint}{(s.IsConnected ? "" : "(down)")}{(s.IsReplica ? "S" : "M")}"));
                Assert.Fail($"expected an Armed event with reason TopologyChanged for the promoted replica {replicaPort}. " +
                            $"Armed events: [{seen}]; multiplexer view: [{views}]; cluster: {Resilience.ResilienceSupport.DescribeLayout()}");
            }
            TimeSpan armedAt;
            lock (gate)
            {
                armedAt = armed.First(a => a.Reason == ArmReason.TopologyChanged && ((IPEndPoint)a.EndPoint).Port == replicaPort).At;
            }
            _out.WriteLine($"[{armedAt}] Armed(TopologyChanged) observed for promoted replica {replicaPort}");

            int[] armedWhenRemoved;
            lock (gate) armedWhenRemoved = armedAtRemoval!;
            Assert.True(armedWhenRemoved.Contains(replicaPort),
                $"{TargetMasterPort} was forgotten (letting the cache serve from L1 again) while {replicaPort}, which now serves its slots, " +
                $"had no armed socket: armed then [{string.Join(",", armedWhenRemoved)}]");

            var settled = await Poll.UntilAsync(
                () => armer.RedirectTargets.Count == 3 && !armer.RedirectTargets.Keys.Any(e => ((IPEndPoint)e).Port == TargetMasterPort),
                TimeSpan.FromSeconds(15));
            Assert.True(settled,
                $"RedirectTargets did not settle back to 3 entries without {TargetMasterPort}: [{string.Join(", ", armer.RedirectTargets.Keys)}]");

            var coherent = await Poll.UntilAsync(() => cache.IsCoherent, TimeSpan.FromSeconds(15));
            Assert.True(coherent, "the cache never reported coherent again after the failover.");

            await sampling.CancelAsync();
            await sampler;
            (TimeSpan At, int[] ArmedPorts)[] unsafeSamples;
            // From the moment the tracker knew 7100 was gone: before that, a coherent cache is a detection delay, not this bug.
            lock (gate) unsafeSamples = coherentSamples.Where(s => s.At > lostAt!.Value && !s.ArmedPorts.Contains(replicaPort)).ToArray();
            Assert.True(unsafeSamples.Length == 0,
                $"the cache reported coherent after losing {TargetMasterPort} while {replicaPort} (serving {TargetMasterPort}'s slots) was not armed, " +
                $"first at {unsafeSamples.FirstOrDefault().At} with armed [{string.Join(",", unsafeSamples.FirstOrDefault().ArmedPorts ?? [])}]");

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
                // A test that failed before the kill left 7100 running: only the pairing may need restoring then.
                if (killed) await RestoreClusterAsync(TargetMasterPort, _out);
                else await ClusterNodes.RestoreDefaultTopologyAsync(_out.WriteLine);
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
    private static async Task RestoreClusterAsync(int port, ITestOutputHelper output)
    {
        ClusterNodes.StartNode(port);

        var up = ClusterNodes.SpinUntil(() => ClusterNodes.PingOk(port), TimeSpan.FromSeconds(30), TimeSpan.FromMilliseconds(200));
        Assert.True(up, $"node {port} did not come back up.");

        var isReplica = ClusterNodes.SpinUntil(() => RoleIsConnectedReplica(port), TimeSpan.FromSeconds(30), TimeSpan.FromMilliseconds(200));
        Assert.True(isReplica, $"node {port} never rejoined as a connected replica of its new master.");

        // Fail back and re-pair the replicas exactly as cluster-up.sh left them, so the redirect-mode cluster tests
        // that run after this one find the topology they assume.
        await ClusterNodes.RestoreDefaultTopologyAsync(output.WriteLine);

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

    private static string[]? RoleLines(int port)
    {
        var r = DockerExec.Run(RedisCli.ClusterContainer, "redis-cli", "-p", port.ToString(), "ROLE");
        if (r.ExitCode != 0) return null;
        return r.StdOut.Split('\n', StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries);
    }
}
