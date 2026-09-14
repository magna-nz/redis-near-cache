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
/// While its connections are down, StackExchange.Redis's Sentinel reconnect holds thread-pool threads (see
/// <see cref="ThreadPoolSetup"/>), so reads in that window can time out on the client: they are retried, and every
/// value a read does return is still checked.
/// </remarks>
public class SentinelFailoverTests
{
    /// <summary>Sentinel: down-after 2 s + election + promotion; generous for loaded CI machines.</summary>
    private static readonly TimeSpan PromotionDeadline = TimeSpan.FromSeconds(60);

    /// <summary>From Sentinel agreeing on the new master until the armer holds a redirect id for it.</summary>
    private static readonly TimeSpan ArmDeadline = TimeSpan.FromSeconds(45);

    /// <summary><c>failover-timeout</c> in <c>sentinel-up.sh</c>: a forced failover cannot have aborted before it.</summary>
    private static readonly TimeSpan SentinelFailoverTimeout = TimeSpan.FromSeconds(5);

    /// <summary>Graceful failovers Sentinel itself aborted that are re-issued before the test gives up.</summary>
    private const int MaxFailoverAttempts = 3;

    /// <summary>Longer than the private multiplexer's 5 s topology check (RedisNearCacheConnection.BuildConfiguration).</summary>
    private static readonly TimeSpan TopologyCheckQuiet = TimeSpan.FromSeconds(7);

    private readonly ITestOutputHelper _out;

    public SentinelFailoverTests(ITestOutputHelper output) => _out = output;

    [Fact]
    public async Task GracefulSentinelFailoverRearmsNewMasterAndFlushes()
    {
        var oldMaster = await SentinelSupport.EnsureHealthyAsync();
        _out.WriteLine("before: " + await SentinelSupport.DescribeAsync());

        await RunFailoverScenarioAsync(oldMaster, "graceful", retryAbortedFailover: true, async () =>
        {
            var (ok, reply) = await SentinelSupport.SentinelFailoverAsync();
            Assert.True(ok, $"SENTINEL FAILOVER {SentinelSupport.ServiceName} was refused: {reply}");
        });
    }

    [Fact]
    public async Task HardMasterKillPromotesReplicaRearmsAndFlushes()
    {
        var oldMaster = await SentinelSupport.EnsureHealthyAsync();
        _out.WriteLine("before: " + await SentinelSupport.DescribeAsync());

        try
        {
            await RunFailoverScenarioAsync(oldMaster, "kill -9", retryAbortedFailover: false, async () =>
            {
                var pid = await SentinelSupport.KillAsync(oldMaster);
                _out.WriteLine($"killed redis-server pid {pid} on {oldMaster}");
            });
        }
        finally
        {
            // Put the killed server back as a replica of whoever is master now, so the next test starts healthy.
            try
            {
                var master = await SentinelSupport.EnsureHealthyAsync();
                _out.WriteLine($"restored: master {master}; {await SentinelSupport.DescribeAsync()}");
            }
            catch (InvalidOperationException ex)
            {
                _out.WriteLine("could not restore the sentinel topology: " + ex.Message);
                throw;
            }
        }
    }

    private async Task RunFailoverScenarioAsync(int oldMaster, string kind, bool retryAbortedFailover, Func<Task> triggerFailover)
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
            _out.WriteLine(ThreadPoolSetup.Describe());

            var clock = new Stopwatch();
            long flushesBefore;
            for (var attempt = 1; ; attempt++)
            {
                // A value cached from the old master, replicated everywhere so the failover cannot lose it. Set up again
                // before a re-issued failover: the aborted one may already have flushed L1, and the flush checked below
                // has to be the one caused by the failover that went through.
                await SentinelSupport.CliAsync(oldMaster, "SET", cachedBefore, "v1");
                foreach (var replica in replicas)
                    Assert.True(await SentinelSupport.ReplicatedAsync(replica, cachedBefore, "v1"), $"{cachedBefore} never reached replica {replica}.");
                Assert.True(await TestHelpers.ReadUntilCachedAsync(cache, cachedBefore, "v1"), $"{cachedBefore} was not cached before the failover.");

                flushesBefore = cache.Statistics.Flushes;
                _out.WriteLine($"before {kind} failover (attempt {attempt}): stats={cache.Statistics}; mux={SentinelSupport.DescribeMultiplexer(p.Multiplexer)}");

                clock.Restart();
                await triggerFailover();

                if (!retryAbortedFailover)
                {
                    newMaster = await SentinelSupport.WaitForNewMasterAsync(oldMaster, PromotionDeadline);
                    break;
                }

                (newMaster, var aborted) = await SentinelSupport.WaitForFailoverOutcomeAsync(oldMaster, SentinelFailoverTimeout, PromotionDeadline);
                if (!aborted || attempt == MaxFailoverAttempts) break;

                _out.WriteLine($"Sentinel aborted the {kind} failover after {clock.ElapsedMilliseconds} ms (attempt {attempt}); re-issuing it: " +
                               await SentinelSupport.DescribeAsync());
                // Everything the aborted attempt set off has to be over before the next one is measured: the replica that
                // was promoted for a moment is resyncing, and the multiplexer may notice the brief promotion (arm + flush)
                // or the demotion (EndpointRemoved + flush) only at its next 5 s topology check. A flush or redirect left
                // over from it would otherwise satisfy properties 1 and 2 for the attempt that goes through.
                Assert.Equal(oldMaster, await SentinelSupport.EnsureHealthyAsync());
                Assert.True(await Chaos.ChaosSupport.QuiesceAsync(cache, stableFor: TopologyCheckQuiet, timeout: TimeSpan.FromSeconds(60)),
                    $"flushes/re-arms kept happening after the aborted failover. stats={cache.Statistics}");
                Assert.True(SentinelSupport.ArmedPorts(p.Armer).SequenceEqual([oldMaster]),
                    $"after the aborted failover the armer did not settle back on {oldMaster} alone (armed: {string.Join(",", SentinelSupport.ArmedPorts(p.Armer))}).");
            }

            Assert.True(newMaster is not null, $"Sentinel never reported a new master after the {kind} failover: {await SentinelSupport.DescribeAsync()}");
            var promoted = newMaster!.Value;
            _out.WriteLine($"sentinels agree on {promoted} after {clock.ElapsedMilliseconds} ms: {await SentinelSupport.DescribeAsync()}");
            Assert.True(await Poll.UntilAsync(() => SentinelSupport.IsMasterAsync(promoted), TimeSpan.FromSeconds(30), TimeSpan.FromMilliseconds(200)),
                $"{promoted} is reported by Sentinel but does not say role:master: {await SentinelSupport.DescribeAsync()}");

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
            await SentinelSupport.CliAsync(promoted, "SET", cachedBefore, "v2");

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
            // pass-through). A local copy of "v1" that survives this is unbounded staleness. Reads that time out while
            // the multiplexer reconnects count as "not yet" (ReadUntilCachedAsync); a returned "v1" never passes.
            if (!await TestHelpers.ReadUntilCachedAsync(cache, cachedBefore, "v2", TimeSpan.FromSeconds(60)))
            {
                var local = cache.TryGetLocal<string>(cachedBefore, out var l) ? l : "<none>";
                Problem($"{cachedBefore} was not read as 'v2' and cached within 60 s (L1 holds '{local}'). stats={cache.Statistics}\n" +
                        await SentinelSupport.DescribeAsync() + "\n" + await SentinelSupport.DescribeNodesAsync(p.Connection.ClientName, cachedBefore));
            }

            // 3. A key first read after the failover is cached, and a foreign write on the new master invalidates it.
            await SentinelSupport.CliAsync(promoted, "SET", readAfter, "v1");
            if (!await TestHelpers.ReadUntilCachedAsync(cache, readAfter, "v1", TimeSpan.FromSeconds(60)))
            {
                Problem($"{readAfter} (written on {promoted} after the failover) was not read and cached within 60 s. stats={cache.Statistics}\n" +
                        await SentinelSupport.DescribeNodesAsync(p.Connection.ClientName, readAfter));
            }
            else
            {
                var hitsBefore = cache.Statistics.Hits;
                // An L1 hit never reaches Redis; the retry only turns a read that did (because L1 lost the key) into the
                // recorded problem below instead of an exception.
                var hit = await Chaos.ChaosSupport.WithReconnectRetryAsync(async () => await cache.GetAsync<string>(readAfter));
                if (hit != "v1" || cache.Statistics.Hits != hitsBefore + 1)
                    Problem($"{readAfter} was not served from L1 after being cached (got '{hit}', hits {hitsBefore} -> {cache.Statistics.Hits}).");

                await SentinelSupport.CliAsync(promoted, "SET", readAfter, "v2");
                var evicted = await Poll.UntilAsync(() => !cache.TryGetLocal<string>(readAfter, out _), TimeSpan.FromSeconds(15));
                if (!evicted)
                {
                    Problem($"a foreign write on the new master {promoted} did not evict {readAfter}: invalidations from it are lost. stats={cache.Statistics}\n" +
                            await SentinelSupport.DescribeNodesAsync(p.Connection.ClientName, readAfter));
                }
                else
                {
                    var after = await Chaos.ChaosSupport.WithReconnectRetryAsync(async () => await cache.GetAsync<string>(readAfter));
                    if (after != "v2") Problem($"{readAfter} read '{after}' after the foreign write of 'v2' on {promoted}.");
                }
            }

            _out.WriteLine($"after {kind} failover: stats={cache.Statistics}; total {clock.ElapsedMilliseconds} ms; {await SentinelSupport.DescribeAsync()}");
            p.Dump(_out);
            dumped = true;
            Assert.True(problems.Count == 0, $"{problems.Count} propert{(problems.Count == 1 ? "y" : "ies")} violated after the {kind} failover:\n" + string.Join("\n", problems));
        }
        catch when (!dumped)
        {
            _out.WriteLine("topology at failure: " + await SentinelSupport.DescribeAsync());
            p.Dump(_out);
            throw;
        }
        finally
        {
            await p.DisposeAsync();
            var master = newMaster ?? await SentinelSupport.AgreedMasterAsync() ?? oldMaster;
            await SentinelSupport.TryCliAsync(master, "DEL", cachedBefore, readAfter);
        }
    }
}
