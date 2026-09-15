using System.Net;
using RedisNearCache.Internal;
using Xunit;
using Xunit.Abstractions;

namespace RedisNearCache.Tests.EdgeCases;

/// <summary>
/// A deployment with a replica in it. Tracking is a master-only concern, so the replica's connections must be
/// invisible to the armer: the "ignore replica connection events" fix means a replica blip must not raise
/// TrackingLost, must not flush L1 and must not change any redirect target. The companion test kills the
/// MASTER's interactive connection in the same topology, which must still re-arm - that is the other half of
/// the <c>IsTrackedMaster</c> logic.
/// </summary>
public class ReplicaConnectionBlipTests
{
    private readonly ITestOutputHelper _out;

    public ReplicaConnectionBlipTests(ITestOutputHelper output) => _out = output;

    [Fact]
    public async Task ReplicaConnectionBlipDoesNotDisableCaching()
    {
        var handle = await EdgeCaseSupport.BuildAsync(EdgeCaseSupport.MasterAndReplicaConnectionString);
        var key = TestHelpers.Key("replica-blip");
        try
        {
            var cache = handle.Cache;
            var masters = handle.Connection.ConnectedMasters().Select(s => s.EndPoint).ToArray();
            _out.WriteLine($"masters={string.Join(",", masters.Select(m => m.ToString()))}, " +
                           $"endpoints={string.Join(",", handle.Multiplexer.GetEndPoints().Select(e => e.ToString()))}");
            Assert.Single(masters);

            await cache.SetAsync(key, "v1");
            Assert.Equal("v1", await cache.GetAsync<string>(key));
            var hitsBefore = cache.Statistics.Hits;
            Assert.Equal("v1", await cache.GetAsync<string>(key));
            Assert.Equal(hitsBefore + 1, cache.Statistics.Hits);

            var redirectsBefore = handle.Armer.RedirectTargets.ToDictionary(kv => kv.Key, kv => kv.Value);
            var flushesBefore = cache.Statistics.Flushes;
            var rearmsBefore = cache.Statistics.Rearms;

            var victims = EdgeCaseSupport.ClientIdsNamed(
                EdgeCaseSupport.Replica("CLIENT", "LIST"), handle.Connection.ClientName);
            _out.WriteLine($"killing {victims.Count} connection(s) named {handle.Connection.ClientName} on the replica");
            Assert.NotEmpty(victims);
            foreach (var id in victims) EdgeCaseSupport.Replica("CLIENT", "KILL", "ID", id.ToString());

            // Wait for the multiplexer to have actually re-established its replica connections (a real
            // condition, not a sleep), with a 2 s ceiling, and only then check that nothing else moved.
            var reconnected = await Poll.UntilAsync(
                () => EdgeCaseSupport.ClientIdsNamed(EdgeCaseSupport.Replica("CLIENT", "LIST"), handle.Connection.ClientName)
                          .Except(victims).Any(),
                TimeSpan.FromSeconds(2),
                TimeSpan.FromMilliseconds(100));
            _out.WriteLine($"replica reconnected={reconnected}; stats={cache.Statistics}");

            Assert.Equal(redirectsBefore, handle.Armer.RedirectTargets.ToDictionary(kv => kv.Key, kv => kv.Value));
            Assert.Equal(flushesBefore, cache.Statistics.Flushes);
            Assert.Equal(rearmsBefore, cache.Statistics.Rearms);

            // L1 was never flushed, so the entry is still there and still served locally.
            Assert.True(cache.TryGetLocal<string>(key, out var local), "the replica blip flushed L1.");
            Assert.Equal("v1", local);
            var hitsAfter = cache.Statistics.Hits;
            Assert.Equal("v1", await cache.GetAsync<string>(key));
            Assert.Equal(hitsAfter + 1, cache.Statistics.Hits);

            // And tracking on the master is untouched: an external write still evicts.
            RedisCli.Standalone("SET", key, "v2");
            var evicted = await Poll.UntilAsync(() => !cache.TryGetLocal<string>(key, out _), TimeSpan.FromSeconds(5));
            Assert.True(evicted, "the master's tracking stopped working after a replica blip.");
            Assert.Equal("v2", await cache.GetAsync<string>(key));
        }
        finally
        {
            await handle.DisposeAsync();
            RedisCli.Standalone("DEL", key);
        }
    }
}

/// <summary>See <see cref="ReplicaConnectionBlipTests"/>: the master half of the same topology.</summary>
public class MasterConnectionKillWithReplicaTests
{
    private readonly ITestOutputHelper _out;

    public MasterConnectionKillWithReplicaTests(ITestOutputHelper output) => _out = output;

    [Fact]
    public async Task MasterConnectionKillWithReplicaPresent()
    {
        var handle = await EdgeCaseSupport.BuildAsync(EdgeCaseSupport.MasterAndReplicaConnectionString);
        var key = TestHelpers.Key("master-kill-with-replica");
        try
        {
            var cache = handle.Cache;
            var masterEndPoint = handle.Connection.ConnectedMasters().Select(s => s.EndPoint).Single();
            var masterServer = handle.Multiplexer.GetServer(masterEndPoint);

            var sawRestore = false;
            handle.Armer.Armed += e =>
            {
                if (e.Reason == ArmReason.InteractiveRestored && Equals(e.EndPoint, masterEndPoint)) sawRestore = true;
            };

            await cache.SetAsync(key, "v1");
            Assert.True(await TestHelpers.ReadUntilCachedAsync(cache, key, "v1"), "key was not cached after the master re-arm.");

            var rearmsBefore = cache.Statistics.Rearms;
            var interactiveId = (long)await masterServer.ExecuteAsync("CLIENT", "ID");
            RedisCli.Standalone("CLIENT", "KILL", "ID", interactiveId.ToString());

            var rearmed = await Poll.UntilAsync(
                () => sawRestore
                      && cache.Statistics.Rearms > rearmsBefore
                      && handle.Armer.RedirectTargets.ContainsKey(masterEndPoint),
                TimeSpan.FromSeconds(15));
            Assert.True(rearmed,
                $"killing the master's interactive connection did not re-arm it while a replica was present. stats={cache.Statistics}");
            _out.WriteLine($"after master kill: {cache.Statistics}");

            // The kill is not a single event: the interactive reconnect re-arms and flushes, and the multiplexer's own
            // reconnect bookkeeping (and the replica pre-arm sweep) can follow a moment later. Let that settle, then
            // reads must be cached again and served as hits. A read that races one last flush is retried rather than
            // failing the test, which is about recovery, not about the exact number of flushes.
            await Chaos.ChaosSupport.QuiesceAsync(cache, TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(15));
            var servedLocally = await Poll.UntilAsync(async () =>
            {
                if (!await TestHelpers.ReadUntilCachedAsync(cache, key, "v1", TimeSpan.FromSeconds(5))) return false;
                var hits = cache.Statistics.Hits;
                return await cache.GetAsync<string>(key) == "v1" && cache.Statistics.Hits == hits + 1;
            }, TimeSpan.FromSeconds(15));
            Assert.True(servedLocally, $"reads were not served locally again after the re-arm. stats={cache.Statistics}");

            RedisCli.Standalone("SET", key, "v2");
            var evicted = await Poll.UntilAsync(() => !cache.TryGetLocal<string>(key, out _), TimeSpan.FromSeconds(5));
            Assert.True(evicted, "tracking did not evict after the master re-arm.");
        }
        finally
        {
            await handle.DisposeAsync();
            RedisCli.Standalone("DEL", key);
        }
    }
}
