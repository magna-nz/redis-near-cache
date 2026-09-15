using System.Diagnostics;
using RedisNearCache.Tests.Chaos;
using RedisNearCache.Tests.EdgeCases;
using StackExchange.Redis;
using Xunit;
using Xunit.Abstractions;

namespace RedisNearCache.Tests.Resilience;

/// <summary>
/// Moving slots between live masters while the cache is reading and a foreign client is writing. This is the
/// coherency case a per-node tracking scheme is most likely to get wrong: a key is tracked on 7100, the slot
/// migrates to 7101, and every later write goes to a node that was never told to track that key for us. If the
/// source node's <c>MIGRATE</c> (which deletes the key locally) or the topology change does not invalidate
/// what L1 holds, the entry stays and is served for the next five minutes.
/// </summary>
/// <remarks>
/// Command used (inside the <c>redis-near-cache-cluster</c> container):
/// <c>redis-cli --cluster reshard 127.0.0.1:7100 --cluster-from &lt;7100 id&gt; --cluster-to &lt;7101 id&gt;
/// --cluster-slots 100 --cluster-yes</c>. <c>redis-cli</c> takes the source's lowest-numbered slots, i.e.
/// 0-99, so the reverse reshard in the <c>finally</c> (from 7101, whose lowest slots are then also 0-99) puts
/// exactly those back and restores the layout the rest of the suite assumes.
/// </remarks>
public class ReshardDuringReadsTests
{
    private const int FromPort = 7100;
    private const int ToPort = 7101;
    private const int SlotsToMove = 100;
    private const int KeyCount = 50;

    private readonly ITestOutputHelper _out;

    public ReshardDuringReadsTests(ITestOutputHelper output) => _out = output;

    [Fact]
    public async Task ReshardDuringReadsNoStale()
    {
        Assert.True(ResilienceSupport.IsDefaultLayout(),
            "the cluster did not start from the default layout: " + ResilienceSupport.DescribeLayout());

        var nodes = ResilienceSupport.ClusterNodes();
        var from = ResilienceSupport.NodeOnPort(nodes, FromPort);
        var to = ResilienceSupport.NodeOnPort(nodes, ToPort);
        Assert.NotNull(from);
        Assert.NotNull(to);

        var handle = await EdgeCaseSupport.BuildAsync(ClusterCacheFixture.ConnectionString);
        var foreign = await ForeignClient.ConnectAsync(ClusterCacheFixture.ConnectionString);
        var forwardOk = false;
        try
        {
            var cache = handle.Cache;
            var keys = KeysOnNode(handle.Multiplexer, from!, KeyCount);
            var migrating = keys.Count(k => handle.Multiplexer.GetHashSlot(k) < SlotsToMove);
            _out.WriteLine($"{keys.Count} keys on {FromPort}, of which {migrating} are in slots 0-{SlotsToMove - 1} and will migrate");
            Assert.True(migrating >= 5, $"only {migrating} keys fall in the slots being moved; the test would not exercise migration.");

            foreach (var key in keys) await foreign.Db.StringSetAsync(key, "0");
            foreach (var key in keys) await cache.GetAsync<string>(key);

            var sw = Stopwatch.StartNew();
            var reshard = Task.Run(() => ResilienceSupport.Reshard(from!.Id, to!.Id, SlotsToMove));

            var outcome = await StressHarness.RunAsync(
                cache,
                keys,
                readerCount: 8,
                duration: TimeSpan.FromSeconds(8),
                writeAsync: (key, value) => ChaosSupport.WithReconnectRetryAsync(
                    () => foreign.Db.StringSetAsync(key, value), TimeSpan.FromSeconds(30)));

            var result = await reshard.WaitAsync(TimeSpan.FromSeconds(120));
            forwardOk = result.ExitCode == 0;
            _out.WriteLine($"reshard exited {result.ExitCode} after {sw.ElapsedMilliseconds} ms; " +
                           $"reads={outcome.Reads} writes={outcome.Writes}; layout now {ResilienceSupport.DescribeLayout()}");
            Assert.True(forwardOk, $"the reshard failed: {result.StdErr}\n{result.StdOut}");

            // The move really happened: 7100 lost 100 slots to 7101.
            var after = ResilienceSupport.ClusterNodes();
            Assert.Equal(5461 - SlotsToMove, SlotCount(after, FromPort));
            Assert.Equal(5462 + SlotsToMove, SlotCount(after, ToPort));

            Assert.True(outcome.Reads > 1_000, $"readers only managed {outcome.Reads} reads; the window was too quiet.");
            Assert.True(outcome.Writes > 50, $"writer only managed {outcome.Writes} writes; the window was too quiet.");

            Assert.True(await ChaosSupport.QuiesceAsync(cache, TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(60)),
                "the cache never settled after the reshard.");

            var stale = await ChaosSupport.FindStaleAsync(cache, foreign, keys);
            _out.WriteLine($"stats after reshard: {cache.Statistics}");
            Assert.True(stale.Count == 0,
                "L1 held values Redis no longer has after slots moved between masters:\n" + string.Join("\n", stale));

            // Tracking on the new owner works: a foreign write to a migrated key still evicts.
            var probe = keys.First(k => handle.Multiplexer.GetHashSlot(k) < SlotsToMove);
            var current = (await foreign.Db.StringGetAsync(probe)).ToString();
            var (recached, report) = await TestHelpers.ReadUntilCachedDiagnosedAsync(cache, probe, current, TimeSpan.FromSeconds(30));
            Assert.True(recached,
                $"a migrated key (slot {handle.Multiplexer.GetHashSlot(probe)}) was not cached again after the reshard: {report}; layout {ResilienceSupport.DescribeLayout()}");
            await foreign.Db.StringSetAsync(probe, "after-reshard");
            var evicted = await Poll.UntilAsync(() => !cache.TryGetLocal<string>(probe, out _), TimeSpan.FromSeconds(15));
            Assert.True(evicted,
                $"a write to a key whose slot moved from {FromPort} to {ToPort} did not evict: the new owner is not tracking it.");

            foreach (var key in keys) await foreign.Db.KeyDeleteAsync(key);
        }
        finally
        {
            await handle.DisposeAsync();
            await foreign.DisposeAsync();

            if (forwardOk)
            {
                var back = ResilienceSupport.Reshard(to!.Id, from!.Id, SlotsToMove);
                _out.WriteLine($"reverse reshard exited {back.ExitCode}; layout now {ResilienceSupport.DescribeLayout()}");
            }

            var restored = await Poll.UntilAsync(
                ResilienceSupport.IsDefaultLayout, TimeSpan.FromSeconds(60), TimeSpan.FromMilliseconds(250));
            Assert.True(restored,
                "the cluster was not restored to the default slot layout (masters 7100-7102 owning 0-5460/5461-10922/10923-16383): "
                + ResilienceSupport.DescribeLayout());
        }
    }

    private static int SlotCount(IReadOnlyList<ClusterNodeInfo> nodes, int port)
    {
        var node = ResilienceSupport.NodeOnPort(nodes, port);
        return node is null ? 0 : node.Slots.Sum(s => s.To - s.From + 1);
    }

    /// <summary>
    /// Keys hashing into one node's slot ranges, deliberately over-sampling the low slots that the reshard will
    /// move. The slot maths is local (<see cref="IConnectionMultiplexer.GetHashSlot"/>), so unlike
    /// <see cref="TestHelpers.KeyForEndPoint"/> this costs no server round trips per candidate.
    /// </summary>
    private static IReadOnlyList<string> KeysOnNode(IConnectionMultiplexer mux, ClusterNodeInfo node, int count)
    {
        var prefix = TestHelpers.Key("reshard");
        var migrating = new List<string>();
        var staying = new List<string>();
        var wantMigrating = count / 4;

        for (var i = 0; i < 500_000 && (migrating.Count < wantMigrating || migrating.Count + staying.Count < count); i++)
        {
            var candidate = $"{prefix}:{i}";
            var slot = mux.GetHashSlot(candidate);
            if (!node.Slots.Any(r => slot >= r.From && slot <= r.To)) continue;
            if (slot < SlotsToMove)
            {
                if (migrating.Count < wantMigrating) migrating.Add(candidate);
            }
            else if (staying.Count < count - wantMigrating)
            {
                staying.Add(candidate);
            }
        }

        var keys = migrating.Concat(staying).ToArray();
        if (keys.Length < count)
            throw new InvalidOperationException($"only found {keys.Length} of {count} keys on node {node.Port}.");
        return keys;
    }
}
