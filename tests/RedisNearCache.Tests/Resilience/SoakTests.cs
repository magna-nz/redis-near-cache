using System.Diagnostics;
using System.Globalization;
using RedisNearCache.Tests.Chaos;
using RedisNearCache.Tests.EdgeCases;
using Xunit;
using Xunit.Abstractions;

namespace RedisNearCache.Tests.Resilience;

/// <summary>
/// The long-running mixed-load soak: readers, writers, a connection kill every 5 s and one <c>FLUSHDB</c>
/// halfway through, then a full staleness audit. Everything it exercises is exercised by a faster test
/// somewhere else; what it adds is duration, because the failures worth finding here are the ones whose
/// probability per event is small - one missed re-arm in a few hundred, one read that stores its reply on the
/// wrong side of an invalidation - and which therefore never show up in a 3 s window.
/// </summary>
/// <remarks>
/// Tagged <c>Soak</c> and excluded from the normal CI filter. It runs for <c>RNC_SOAK_SECONDS</c> seconds
/// (default 30, so it is usable locally as a smoke test); the manual CI job sets 1800.
/// <c>FLUSHDB</c> wipes the whole standalone database, hence the named collection - the assembly already
/// disables parallelization, but the intent should survive that changing.
/// </remarks>
[Collection("flush")]
[Trait("Category", "Soak")]
public class SoakTests
{
    private const int Readers = 8;
    private const int KeyCount = 50;
    private static readonly TimeSpan KillInterval = TimeSpan.FromSeconds(5);

    private readonly ITestOutputHelper _out;

    public SoakTests(ITestOutputHelper output) => _out = output;

    private static TimeSpan Duration =>
        TimeSpan.FromSeconds(int.TryParse(
            Environment.GetEnvironmentVariable("RNC_SOAK_SECONDS"),
            NumberStyles.Integer, CultureInfo.InvariantCulture, out var seconds) && seconds > 0
            ? seconds
            : 30);

    [Fact]
    public async Task Soak_ThirtyMinutesMixedLoad()
    {
        var duration = Duration;
        var handle = await EdgeCaseSupport.BuildAsync(StandaloneCacheFixture.ConnectionString);
        var foreign = await ForeignClient.ConnectAsync(StandaloneCacheFixture.ConnectionString);
        var keys = StressHarness.KeyPool("soak", KeyCount);
        try
        {
            var cache = handle.Cache;
            _out.WriteLine($"soak for {duration.TotalSeconds:0} s (RNC_SOAK_SECONDS), {Readers} readers over {KeyCount} keys");

            foreach (var key in keys) await foreign.Db.StringSetAsync(key, "0");
            foreach (var key in keys) await cache.GetAsync<string>(key);

            var sw = Stopwatch.StartNew();
            var kills = 0;
            var flushed = false;
            using var stop = new CancellationTokenSource(duration);

            var killer = Task.Run(async () =>
            {
                while (!stop.IsCancellationRequested)
                {
                    try { await Task.Delay(KillInterval, stop.Token); }
                    catch (OperationCanceledException) { return; }

                    try
                    {
                        var ids = EdgeCaseSupport.ClientIdsNamed(
                            RedisCli.Standalone("CLIENT", "LIST"), handle.Connection.ClientName);
                        foreach (var id in ids)
                        {
                            RedisCli.Standalone("CLIENT", "KILL", "ID", id.ToString(CultureInfo.InvariantCulture));
                            Interlocked.Increment(ref kills);
                        }
                    }
                    catch (InvalidOperationException)
                    {
                        // redis-cli raced a kill of its own target; the next round will catch up.
                    }
                }
            });

            var flusher = Task.Run(async () =>
            {
                try { await Task.Delay(duration / 2, stop.Token); }
                catch (OperationCanceledException) { return; }
                RedisCli.Standalone("FLUSHDB");
                flushed = true;
                // Put the keys back so the second half still reads existing keys.
                foreach (var key in keys) await foreign.Db.StringSetAsync(key, "0");
            });

            var outcome = await StressHarness.RunAsync(
                cache,
                keys,
                readerCount: Readers,
                duration: duration,
                writeAsync: (key, value) => ChaosSupport.WithReconnectRetryAsync(
                    () => foreign.Db.StringSetAsync(key, value), TimeSpan.FromSeconds(60)));

            await stop.CancelAsync();
            await Task.WhenAll(killer, flusher);

            _out.WriteLine($"ran {sw.Elapsed.TotalSeconds:0.0} s: reads={outcome.Reads} writes={outcome.Writes} " +
                           $"connection kills={kills} flushdb={flushed} readerFailures={outcome.ReaderFailures.Count}");
            _out.WriteLine($"statistics: {cache.Statistics}");

            Assert.True(flushed, "the mid-run FLUSHDB never happened.");
            Assert.True(kills >= 1, "no connections were killed during the soak.");
            Assert.True(outcome.Reads > 1_000, $"readers only managed {outcome.Reads} reads over {duration.TotalSeconds:0} s.");
            Assert.True(outcome.Writes > 50, $"writer only managed {outcome.Writes} writes over {duration.TotalSeconds:0} s.");

            Assert.True(await ChaosSupport.QuiesceAsync(cache, TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(60)),
                "the cache never settled after the soak.");

            var stale = await ChaosSupport.FindStaleAsync(cache, foreign, keys);
            _out.WriteLine($"final statistics: {cache.Statistics}; stale={stale.Count}");
            Assert.True(stale.Count == 0,
                $"L1 held values Redis no longer has after a {duration.TotalSeconds:0} s soak:\n" + string.Join("\n", stale));

            // Still coherent at the end, not merely quiet.
            var probe = keys[0];
            var current = (await foreign.Db.StringGetAsync(probe)).ToString();
            Assert.True(await TestHelpers.ReadUntilCachedAsync(cache, probe, current, TimeSpan.FromSeconds(30)),
                "no key could be cached again at the end of the soak.");
            await foreign.Db.StringSetAsync(probe, "end-of-soak");
            var evicted = await Poll.UntilAsync(() => !cache.TryGetLocal<string>(probe, out _), TimeSpan.FromSeconds(15));
            Assert.True(evicted, "invalidations were not being delivered by the end of the soak.");
        }
        finally
        {
            await handle.DisposeAsync();
            foreach (var key in keys) await foreign.Db.KeyDeleteAsync(key);
            await foreign.DisposeAsync();
        }
    }
}
