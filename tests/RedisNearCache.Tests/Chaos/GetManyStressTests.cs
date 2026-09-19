using System.Collections.Concurrent;
using System.Globalization;
using System.Reflection;
using RedisNearCache.Tests.EdgeCases;
using StackExchange.Redis;
using Xunit;
using Xunit.Abstractions;

namespace RedisNearCache.Tests.Chaos;

/// <summary>
/// The stale-read question of <see cref="StressNoStaleAfterQuiescenceTests"/>, asked of the multi-key read:
/// several foreign writers rewriting a small key set while several readers loop
/// <see cref="IRedisNearCache.GetManyAsync{T}"/> over the WHOLE set, so every call races a write and the
/// in-flight rule fires hundreds of times. Once the writers stop and the system has quiesced, L1 must hold
/// exactly what Redis holds for every key.
/// </summary>
/// <remarks>
/// Not <see cref="StressHarness"/>: that harness has one writer and reads one key at a time, and the point here
/// is several writers against a read that touches every key at once. Its conventions are kept - foreign writes, a
/// bounded window, an explicit yield in the reader loop so the invalidation callbacks are not starved, failures
/// collected rather than thrown on a background thread, and <see cref="ChaosSupport.QuiesceAsync"/> before the
/// final assertion.
/// <para>
/// No per-reader monotonicity is claimed: several writers race each other and each reader sees whatever the
/// server answered, so the only claim about a value observed DURING the run is that some writer wrote it.
/// </para>
/// </remarks>
public class GetManyStressTests
{
    private const int KeyCount = 12;
    private const int ReaderCount = 8;
    private const int WriterCount = 4;

    private readonly ITestOutputHelper _out;

    public GetManyStressTests(ITestOutputHelper output) => _out = output;

    [Fact]
    public async Task NoStaleValueSurvivesAMultiKeyReadStorm()
    {
        var keys = StressHarness.KeyPool("gm-stress", KeyCount);
        var handle = await EdgeCaseSupport.BuildAsync(StandaloneCacheFixture.ConnectionString);
        await using var foreign = await ForeignClient.ConnectAsync(StandaloneCacheFixture.ConnectionString);
        try
        {
            var cache = handle.Cache;
            var outcome = await RunStormAsync(cache, foreign, keys, TimeSpan.FromSeconds(5), writerPacing: TimeSpan.FromMilliseconds(1));

            Assert.True(outcome.Failures.IsEmpty, "readers threw: " + string.Join(" | ", outcome.Failures.Select(e => e.ToString()).Take(3)));
            Assert.True(outcome.Writes > 500, $"only {outcome.Writes} writes; the window was too quiet to be a stress test.");
            Assert.True(outcome.Reads > 500, $"only {outcome.Reads} multi-key reads; the window was too quiet to be a stress test.");
            Assert.True(outcome.Violations.IsEmpty, "a multi-key read answered with something no writer wrote: " + string.Join(" | ", outcome.Violations.Take(3)));
            Assert.True(cache.Statistics.Invalidations > 100, $"only {cache.Statistics.Invalidations} invalidations arrived; the readers were not racing the writers.");
            Assert.True(cache.Statistics.RaceDiscards > 0, "not one reply was discarded for arriving after an invalidation; the window never actually raced.");

            Assert.True(await ChaosSupport.QuiesceAsync(cache, TimeSpan.FromSeconds(1)),
                "invalidations were still arriving after the settle deadline; the system never quiesced.");

            // Quiescence means nothing is still on its way, so from here a multi-key read must both agree with
            // Redis for every key AND leave every key in L1 - without the second half, the staleness check below
            // would pass over an empty cache and prove nothing.
            var settled = await Poll.UntilAsync(async () =>
            {
                var result = await cache.GetManyAsync<string>(keys);
                foreach (var key in keys)
                {
                    var truth = await foreign.Db.StringGetAsync(key);
                    if (!string.Equals(result[key], truth.ToString(), StringComparison.Ordinal)) return false;
                    if (!cache.TryGetLocal<string>(key, out _)) return false;
                }

                return true;
            }, TimeSpan.FromSeconds(15));

            _out.WriteLine($"reads={outcome.Reads} writes={outcome.Writes} local={keys.Count(k => cache.TryGetLocal<string>(k, out _))}/{keys.Count} stats={cache.Statistics}{L1SizeDiagnostic(cache)}");
            Assert.True(settled,
                "after quiescence a multi-key read still did not leave every key in L1 holding what Redis holds. " +
                "If every key is missing from L1 rather than holding a wrong value, see " +
                nameof(L1StillAcceptsNewKeysAfterAHighRateStorm) + " in this file." +
                L1SizeDiagnostic(cache));

            var stale = await ChaosSupport.FindStaleAsync(cache, foreign, keys);
            Assert.True(stale.Count == 0, "L1 held a value Redis no longer has after quiescence: " + string.Join(" | ", stale));
        }
        finally
        {
            await foreign.Db.KeyDeleteAsync(keys.Select(k => (RedisKey)k).ToArray());
            await handle.DisposeAsync();
        }
    }

    /// <summary>
    /// After an unpaced write storm on a handful of hot keys the L1 store must still accept new entries: a key the
    /// cache has never seen, created after the storm, is cached like any other miss.
    /// </summary>
    /// <remarks>
    /// This is the integration-level guard for the workaround in <see cref="RedisNearCache.Caching.L1Cache"/>
    /// (its remarks have the mechanism; <c>L1CacheSizeAccountingTests</c> is the unit-level guard). Before it,
    /// <c>MemoryCache</c>'s size total drifted below zero under concurrent stores and removes of the same key
    /// (observed here: <c>Count</c> 0, <c>Size</c> -4 to -6, <c>SizeLimit</c> 10,000) and every later store was
    /// refused, silently and for good, while <see cref="IRedisNearCache.IsCoherent"/> stayed true and no counter in
    /// <see cref="RedisNearCacheStatistics"/> moved. It was never specific to the multi-key read - a plain
    /// <see cref="IRedisNearCache.GetAsync{T}"/> has the same L1 underneath - but one call reading every key reaches
    /// it easily, where <see cref="StressNoStaleAfterQuiescenceTests"/>, whose single writer awaits each write,
    /// never did.
    /// </remarks>
    [Fact]
    public async Task L1StillAcceptsNewKeysAfterAHighRateStorm()
    {
        var keys = StressHarness.KeyPool("gm-stress-hot", KeyCount);
        var fresh = TestHelpers.Key("gm-stress-fresh");
        var handle = await EdgeCaseSupport.BuildAsync(StandaloneCacheFixture.ConnectionString);
        await using var foreign = await ForeignClient.ConnectAsync(StandaloneCacheFixture.ConnectionString);
        try
        {
            var cache = handle.Cache;
            var outcome = await RunStormAsync(cache, foreign, keys, TimeSpan.FromSeconds(5), writerPacing: null);

            Assert.True(outcome.Failures.IsEmpty, "readers threw: " + string.Join(" | ", outcome.Failures.Select(e => e.ToString()).Take(3)));
            Assert.True(outcome.Violations.IsEmpty, "a multi-key read answered with something no writer wrote: " + string.Join(" | ", outcome.Violations.Take(3)));
            Assert.True(cache.Statistics.Hits > 0, "nothing was ever served from L1, so the cache was never working in the first place.");
            Assert.True(await ChaosSupport.QuiesceAsync(cache, TimeSpan.FromSeconds(1)), "the system never quiesced.");

            // A key this cache has never read, created after the storm: nothing about it is contended, and the
            // cache says it is coherent, so it must be cached like any other miss.
            await foreign.Db.StringSetAsync(fresh, "fresh");
            Assert.Equal("fresh", await cache.GetAsync<string>(fresh));
            Assert.True(cache.IsCoherent, "the cache reported itself incoherent, which would at least be visible to a caller.");

            _out.WriteLine($"reads={outcome.Reads} writes={outcome.Writes} stats={cache.Statistics}{L1SizeDiagnostic(cache)}");
            Assert.True(cache.TryGetLocal<string>(fresh, out _),
                "after a high-rate write storm the L1 store refused an uncontended key read for the first time: " +
                "MemoryCache's size accounting has drifted again; see the remarks on L1Cache." + L1SizeDiagnostic(cache));
        }
        finally
        {
            await foreign.Db.KeyDeleteAsync([.. keys.Select(k => (RedisKey)k), fresh]);
            await handle.DisposeAsync();
        }
    }

    private sealed class StormOutcome
    {
        public long Reads;
        public long Writes;
        public ConcurrentBag<Exception> Failures { get; } = [];
        public ConcurrentBag<string> Violations { get; } = [];
    }

    private static async Task<StormOutcome> RunStormAsync(
        IRedisNearCache cache,
        ForeignClient foreign,
        IReadOnlyList<string> keys,
        TimeSpan duration,
        TimeSpan? writerPacing)
    {
        var outcome = new StormOutcome();

        // Every value any writer has ever attempted, recorded BEFORE the write goes out, so a reader can never
        // legitimately observe a value that is not in here.
        var written = new ConcurrentDictionary<string, byte>(StringComparer.Ordinal) { ["seed"] = 0 };
        foreach (var key in keys) await foreign.Db.StringSetAsync(key, "seed");

        using var stop = new CancellationTokenSource();
        var readers = Enumerable.Range(0, ReaderCount).Select(_ => Task.Run(async () =>
        {
            while (!stop.IsCancellationRequested)
            {
                try
                {
                    var result = await cache.GetManyAsync<string>(keys);
                    Interlocked.Increment(ref outcome.Reads);
                    if (result.Count != keys.Count) outcome.Violations.Add($"a read returned {result.Count} entries for {keys.Count} keys.");
                    foreach (var (key, value) in result)
                    {
                        if (value is null) outcome.Violations.Add($"{key}: null, although the key is never deleted.");
                        else if (!written.ContainsKey(value)) outcome.Violations.Add($"{key}: '{value}' was never written by any writer.");
                    }
                }
                catch (Exception ex)
                {
                    outcome.Failures.Add(ex);
                }

                // A call made entirely of hits completes synchronously; without a yield the readers would
                // monopolise the thread pool and starve the invalidation callbacks under test.
                await Task.Yield();
            }
        })).ToArray();

        var deadline = DateTime.UtcNow + duration;
        var writers = Enumerable.Range(0, WriterCount).Select(w => Task.Run(async () =>
        {
            var rng = new Random(9176 + w);
            long counter = 0;
            while (DateTime.UtcNow < deadline)
            {
                var key = keys[rng.Next(keys.Count)];
                var value = $"w{w}-{(++counter).ToString(CultureInfo.InvariantCulture)}";
                written[value] = 0;
                await foreign.Db.StringSetAsync(key, value);
                Interlocked.Increment(ref outcome.Writes);
                if (writerPacing is { } pacing) await Task.Delay(pacing);
            }
        })).ToArray();

        await Task.WhenAll(writers);
        await stop.CancelAsync();
        await Task.WhenAll(readers);
        return outcome;
    }

    /// <summary>
    /// The L1 <c>MemoryCache</c>'s own entry count and size counter, read reflectively for failure messages only:
    /// when the two disagree (or the size is negative) the failure is the accounting drift described on
    /// <see cref="L1StillAcceptsNewKeysAfterAHighRateStorm"/> rather than anything about this library.
    /// </summary>
    private static string L1SizeDiagnostic(IRedisNearCache cache)
    {
        try
        {
            var l1 = cache.GetType().GetField("_l1", BindingFlags.Instance | BindingFlags.NonPublic)?.GetValue(cache);
            var memoryCache = l1?.GetType().GetField("_cache", BindingFlags.Instance | BindingFlags.NonPublic)?.GetValue(l1);
            if (memoryCache is null) return string.Empty;
            var count = memoryCache.GetType().GetProperty("Count", BindingFlags.Instance | BindingFlags.Public)?.GetValue(memoryCache);
            var size = memoryCache.GetType().GetProperty("Size", BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic)?.GetValue(memoryCache);
            return $" [MemoryCache Count={count} Size={size}]";
        }
        catch (Exception ex)
        {
            return $" [MemoryCache internals unreadable: {ex.GetType().Name}]";
        }
    }
}
