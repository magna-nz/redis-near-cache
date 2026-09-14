using RedisNearCache.Tests.Chaos;
using RedisNearCache.Tests.EdgeCases;
using Xunit;
using Xunit.Abstractions;

namespace RedisNearCache.Tests.Resilience;

/// <summary>
/// The subscriber connection is the single point of failure for coherency: the server redirects invalidations
/// to its client id, and when it reconnects it comes back with a NEW id while the server keeps redirecting to
/// the dead one. Nothing errors, nothing logs, invalidations simply stop arriving. Killing it once is already
/// covered (<see cref="KillSubscriberRearmsTests"/>); this kills it ten times in a row while eight readers and
/// a foreign writer are both going flat out, so every re-arm has to land in the middle of live traffic, with
/// reads in flight across the gap. If a single re-arm is missed or races a read that then stores its reply,
/// the final audit finds a key whose L1 copy no longer matches Redis.
/// </summary>
public class SubscriberKillUnderLoadTests
{
    private const int Kills = 10;
    private const int Readers = 8;
    private const int KeyCount = 20;
    private static readonly TimeSpan KillInterval = TimeSpan.FromMilliseconds(300);

    private readonly ITestOutputHelper _out;

    public SubscriberKillUnderLoadTests(ITestOutputHelper output) => _out = output;

    [Fact]
    public async Task SubscriberConnectionKilledRepeatedlyUnderLoad()
    {
        var handle = await EdgeCaseSupport.BuildAsync(StandaloneCacheFixture.ConnectionString);
        var foreign = await ForeignClient.ConnectAsync(StandaloneCacheFixture.ConnectionString);
        var keys = StressHarness.KeyPool("subscriber-kill-load", KeyCount);
        try
        {
            var cache = handle.Cache;
            foreach (var key in keys) await foreign.Db.StringSetAsync(key, "0");

            // Warm L1 so the run starts from "everything cached", which is the state a lost invalidation
            // would leave stale.
            foreach (var key in keys) await cache.GetAsync<string>(key);

            var rearmsBefore = cache.Statistics.Rearms;
            var killed = 0;
            var killer = Task.Run(async () =>
            {
                for (var i = 0; i < Kills; i++)
                {
                    var ids = ResilienceSupport.SubscriberIdsNamed(
                        RedisCli.Standalone("CLIENT", "LIST"), handle.Connection.ClientName);
                    foreach (var id in ids)
                    {
                        RedisCli.Standalone("CLIENT", "KILL", "ID", id.ToString());
                        Interlocked.Increment(ref killed);
                    }

                    await Task.Delay(KillInterval);
                }
            });

            // The readers/writer window has to outlast the kills; the harness stops when its writer's
            // duration elapses, so give it the kill schedule plus a margin for the last re-arm.
            var outcome = await StressHarness.RunAsync(
                cache,
                keys,
                readerCount: Readers,
                duration: KillInterval * Kills + TimeSpan.FromSeconds(2),
                writeAsync: (key, value) => foreign.Db.StringSetAsync(key, value));

            await killer;

            _out.WriteLine($"reads={outcome.Reads} writes={outcome.Writes} subscriber kills={killed} stats={cache.Statistics}");
            Assert.True(killed >= Kills, $"only {killed} subscriber connection(s) were killed; expected at least {Kills}.");
            Assert.True(outcome.Reads > 1_000, $"readers only managed {outcome.Reads} reads; the window was too quiet.");
            Assert.True(outcome.Writes > 100, $"writer only managed {outcome.Writes} writes; the window was too quiet.");

            // Every kill must produce its own re-arm. Poll: the last one can still be in flight.
            var rearmed = await Poll.UntilAsync(
                () => cache.Statistics.Rearms >= rearmsBefore + Kills,
                TimeSpan.FromSeconds(30), TimeSpan.FromMilliseconds(100));
            Assert.True(rearmed,
                $"expected at least {Kills} re-arms after {killed} subscriber kills but saw {cache.Statistics.Rearms - rearmsBefore}. " +
                "A kill that does not re-arm leaves the server redirecting invalidations to a dead client id.");

            Assert.True(await ChaosSupport.QuiesceAsync(cache, TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(30)),
                "the cache never settled after the kill storm.");

            var stale = await ChaosSupport.FindStaleAsync(cache, foreign, keys);
            Assert.True(stale.Count == 0,
                "L1 served values Redis no longer holds after repeated subscriber kills under load:\n" + string.Join("\n", stale));

            // And the connection that survived the storm is genuinely tracked, not just quiet.
            var probe = keys[0];
            Assert.True(await TestHelpers.ReadUntilCachedAsync(cache, probe, (await foreign.Db.StringGetAsync(probe)).ToString(), TimeSpan.FromSeconds(15)),
                "the key was not cached again after the kill storm.");
            await foreign.Db.StringSetAsync(probe, "final");
            var evicted = await Poll.UntilAsync(() => !cache.TryGetLocal<string>(probe, out _), TimeSpan.FromSeconds(10));
            Assert.True(evicted, "invalidations stopped arriving after the last subscriber kill.");
        }
        finally
        {
            await handle.DisposeAsync();
            foreach (var key in keys) await foreign.Db.KeyDeleteAsync(key);
            await foreign.DisposeAsync();
        }
    }
}
