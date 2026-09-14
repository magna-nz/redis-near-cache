using System.Diagnostics;
using Xunit;
using Xunit.Abstractions;

namespace RedisNearCache.Tests.Sentinel;

/// <summary>
/// Sentinel promotes a replica. For this library that is the dangerous moment: tracking is per node, and the
/// promoted replica has never had <c>CLIENT TRACKING ON REDIRECT</c> issued on it. Until the armer arms it, writes
/// there produce no invalidation, and every L1 entry read from the old master is unprotected - so the properties
/// asserted after both a graceful and a hard failover are: the new master gets armed, L1 is flushed (a value
/// cached before the failover and changed on the new master afterwards is not served), and a key read after the
/// failover is invalidated by a foreign write on the new master.
/// </summary>
/// <remarks>
/// The tests start from whatever master Sentinel currently reports (<see cref="SentinelSupport.EnsureHealthyAsync"/>
/// also restarts a server a previous run killed), so they are re-runnable in any order without failing back.
/// The multiplexer learns about the switch from Sentinel's <c>+switch-master</c> message or its 5 s topology check.
/// </remarks>
public class SentinelFailoverTests
{
    /// <summary>Sentinel: down-after 2 s + election + promotion; generous for loaded CI machines.</summary>
    private static readonly TimeSpan PromotionDeadline = TimeSpan.FromSeconds(60);

    /// <summary>From Sentinel agreeing on the new master until the armer holds a redirect id for it.</summary>
    private static readonly TimeSpan ArmDeadline = TimeSpan.FromSeconds(45);

    private readonly ITestOutputHelper _out;

    public SentinelFailoverTests(ITestOutputHelper output) => _out = output;

    [Fact]
    public async Task GracefulSentinelFailoverRearmsNewMasterAndFlushes()
    {
        var oldMaster = await SentinelSupport.EnsureHealthyAsync();
        _out.WriteLine("before: " + SentinelSupport.Describe());

        await RunFailoverScenarioAsync(oldMaster, "graceful", async () =>
        {
            var (ok, reply) = await SentinelSupport.SentinelFailoverAsync();
            Assert.True(ok, $"SENTINEL FAILOVER {SentinelSupport.ServiceName} was refused: {reply}");
        });
    }

    [Fact]
    public async Task HardMasterKillPromotesReplicaRearmsAndFlushes()
    {
        var oldMaster = await SentinelSupport.EnsureHealthyAsync();
        _out.WriteLine("before: " + SentinelSupport.Describe());

        try
        {
            await RunFailoverScenarioAsync(oldMaster, "kill -9", () =>
            {
                var pid = SentinelSupport.Kill(oldMaster);
                _out.WriteLine($"killed redis-server pid {pid} on {oldMaster}");
                return Task.CompletedTask;
            });
        }
        finally
        {
            // Put the killed server back as a replica of whoever is master now, so the next test starts healthy.
            try
            {
                var master = await SentinelSupport.EnsureHealthyAsync();
                _out.WriteLine($"restored: master {master}; {SentinelSupport.Describe()}");
            }
            catch (InvalidOperationException ex)
            {
                _out.WriteLine("could not restore the sentinel topology: " + ex.Message);
                throw;
            }
        }
    }

    private async Task RunFailoverScenarioAsync(int oldMaster, string kind, Func<Task> triggerFailover)
    {
        var replicas = SentinelSupport.DataPorts.Where(port => port != oldMaster).ToArray();
        var cachedBefore = TestHelpers.Key("sentinel-before");
        var readAfter = TestHelpers.Key("sentinel-after");

        var p = await SentinelSupport.BuildAsync();
        int? newMaster = null;
        var dumped = false;
        try
        {
            var cache = p.Cache;
            Assert.Equal([oldMaster], SentinelSupport.ArmedPorts(p.Armer));

            // A value cached from the old master, replicated everywhere so the failover cannot lose it.
            SentinelSupport.Cli(oldMaster, "SET", cachedBefore, "v1");
            foreach (var replica in replicas)
                Assert.True(await SentinelSupport.ReplicatedAsync(replica, cachedBefore, "v1"), $"{cachedBefore} never reached replica {replica}.");
            Assert.True(await TestHelpers.ReadUntilCachedAsync(cache, cachedBefore, "v1"), $"{cachedBefore} was not cached before the failover.");

            var flushesBefore = cache.Statistics.Flushes;
            _out.WriteLine($"before {kind} failover: stats={cache.Statistics}; mux={SentinelSupport.DescribeMultiplexer(p.Multiplexer)}");

            var clock = Stopwatch.StartNew();
            await triggerFailover();

            newMaster = await SentinelSupport.WaitForNewMasterAsync(oldMaster, PromotionDeadline);
            Assert.True(newMaster is not null, $"Sentinel never reported a new master after the {kind} failover: {SentinelSupport.Describe()}");
            var promoted = newMaster!.Value;
            _out.WriteLine($"sentinels agree on {promoted} after {clock.ElapsedMilliseconds} ms: {SentinelSupport.Describe()}");
            Assert.True(await Poll.UntilAsync(() => SentinelSupport.IsMaster(promoted), TimeSpan.FromSeconds(30), TimeSpan.FromMilliseconds(200)),
                $"{promoted} is reported by Sentinel but does not say role:master: {SentinelSupport.Describe()}");

            // Every property below is checked and recorded rather than asserted one at a time, so a single run shows
            // which of them hold; the test fails at the end if any did not.
            var problems = new List<string>();
            void Problem(string message)
            {
                problems.Add($"[{clock.ElapsedMilliseconds} ms] {message}");
                _out.WriteLine($"PROBLEM at {clock.ElapsedMilliseconds} ms: {message}");
            }

            // The pre-failover value changes on the new master. The key was only ever tracked on the old master, so
            // no invalidation for it can come from the new one: only an L1 flush keeps "v1" from being served.
            SentinelSupport.Cli(promoted, "SET", cachedBefore, "v2");

            // 1. The new master gets armed.
            var armed = await Poll.UntilAsync(() => SentinelSupport.RedirectFor(p.Armer, promoted) is not null, ArmDeadline, TimeSpan.FromMilliseconds(200));
            _out.WriteLine($"armed ports after {clock.ElapsedMilliseconds} ms: {string.Join(",", SentinelSupport.ArmedPorts(p.Armer))}; " +
                           $"mux={SentinelSupport.DescribeMultiplexer(p.Multiplexer)}; stats={cache.Statistics}");
            if (!armed)
                Problem($"the promoted master {promoted} was never armed (armed: {string.Join(",", SentinelSupport.ArmedPorts(p.Armer))}; " +
                        $"mux: {SentinelSupport.DescribeMultiplexer(p.Multiplexer)}), so its writes produce no invalidations.");

            // 2. L1 was flushed: the pre-failover copy is gone (nothing has read the key since, so nothing re-populated it).
            var flushed = await Poll.UntilAsync(
                () => !(cache.TryGetLocal<string>(cachedBefore, out var local) && local == "v1"),
                TimeSpan.FromSeconds(10), TimeSpan.FromMilliseconds(100));
            if (!flushed) Problem($"L1 still holds the pre-failover value of {cachedBefore} after the failover. stats={cache.Statistics}");
            if (cache.Statistics.Flushes <= flushesBefore) Problem($"no L1 flush was counted across the failover. stats={cache.Statistics}");

            // 2b. Reads converge on the value Redis now holds everywhere, and caching resumes (no endpoint left stuck in
            // pass-through). A local copy of "v1" that survives this is unbounded staleness.
            if (!await TestHelpers.ReadUntilCachedAsync(cache, cachedBefore, "v2", TimeSpan.FromSeconds(60)))
            {
                var local = cache.TryGetLocal<string>(cachedBefore, out var l) ? l : "<none>";
                Problem($"{cachedBefore} was not read as 'v2' and cached within 60 s (L1 holds '{local}'). stats={cache.Statistics}\n" +
                        SentinelSupport.Describe() + "\n" + SentinelSupport.DescribeNodes(p.Connection.ClientName, cachedBefore));
            }

            // 3. A key first read after the failover is cached, and a foreign write on the new master invalidates it.
            SentinelSupport.Cli(promoted, "SET", readAfter, "v1");
            if (!await TestHelpers.ReadUntilCachedAsync(cache, readAfter, "v1", TimeSpan.FromSeconds(60)))
            {
                Problem($"{readAfter} (written on {promoted} after the failover) was not read and cached within 60 s. stats={cache.Statistics}\n" +
                        SentinelSupport.DescribeNodes(p.Connection.ClientName, readAfter));
            }
            else
            {
                var hitsBefore = cache.Statistics.Hits;
                var hit = await cache.GetAsync<string>(readAfter);
                if (hit != "v1" || cache.Statistics.Hits != hitsBefore + 1)
                    Problem($"{readAfter} was not served from L1 after being cached (got '{hit}', hits {hitsBefore} -> {cache.Statistics.Hits}).");

                SentinelSupport.Cli(promoted, "SET", readAfter, "v2");
                var evicted = await Poll.UntilAsync(() => !cache.TryGetLocal<string>(readAfter, out _), TimeSpan.FromSeconds(15));
                if (!evicted)
                {
                    Problem($"a foreign write on the new master {promoted} did not evict {readAfter}: invalidations from it are lost. stats={cache.Statistics}\n" +
                            SentinelSupport.DescribeNodes(p.Connection.ClientName, readAfter));
                }
                else
                {
                    var after = await Chaos.ChaosSupport.WithReconnectRetryAsync(async () => await cache.GetAsync<string>(readAfter));
                    if (after != "v2") Problem($"{readAfter} read '{after}' after the foreign write of 'v2' on {promoted}.");
                }
            }

            _out.WriteLine($"after {kind} failover: stats={cache.Statistics}; total {clock.ElapsedMilliseconds} ms; {SentinelSupport.Describe()}");
            p.Dump(_out);
            dumped = true;
            Assert.True(problems.Count == 0, $"{problems.Count} propert{(problems.Count == 1 ? "y" : "ies")} violated after the {kind} failover:\n" + string.Join("\n", problems));
        }
        catch when (!dumped)
        {
            _out.WriteLine("topology at failure: " + SentinelSupport.Describe());
            p.Dump(_out);
            throw;
        }
        finally
        {
            await p.DisposeAsync();
            var master = newMaster ?? SentinelSupport.AgreedMaster() ?? oldMaster;
            SentinelSupport.TryCli(master, "DEL", cachedBefore, readAfter);
        }
    }
}
