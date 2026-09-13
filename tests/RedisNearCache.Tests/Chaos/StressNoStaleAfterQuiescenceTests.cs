using Xunit;
using Xunit.Abstractions;

namespace RedisNearCache.Tests.Chaos;

/// <summary>
/// The headline chaos question: with 32 readers hammering a small pool of keys while a foreign client
/// rewrites them, can L1 be left holding a value that Redis no longer has once everything has settled?
/// </summary>
/// <remarks>
/// KNOWN DEFECT — it can. This test fails intermittently, and always with the same shape: one key of the
/// twenty is left holding a value from early in the run (observed: <c>L1='29'</c> while Redis held
/// <c>'1822'</c>), which is then served as a hit for the next five minutes.
///
/// Same root cause as <see cref="InvalidationStormHotKeyTests"/>, which documents the interleaving in full
/// and which <see cref="InvalidationOrderingWindowTests.EvictBeforeMarkLosesTheInvalidation"/> reproduces
/// deterministically: <c>OnKeyInvalidated</c> (src/RedisNearCache/Caching/RedisNearCache.cs:71-72) evicts L1
/// before it marks the in-flight tracker, so a reader that stores between those two statements keeps a value
/// the server has already superseded and stopped tracking.
///
/// How often it fails depends on how contended the machine is, because the defect needs the invalidation
/// callback to be pre-empted between two adjacent statements. On an idle machine six consecutive attempts
/// all passed; with another build running concurrently it failed roughly one attempt in four.
/// </remarks>
public class StressNoStaleAfterQuiescenceTests : IClassFixture<StandaloneCacheFixture>, IAsyncLifetime
{
    private readonly StandaloneCacheFixture _fx;
    private readonly ITestOutputHelper _out;
    private ForeignClient _foreign = null!;

    public StressNoStaleAfterQuiescenceTests(StandaloneCacheFixture fx, ITestOutputHelper output)
    {
        _fx = fx;
        _out = output;
    }

    public async Task InitializeAsync() =>
        _foreign = await ForeignClient.ConnectAsync(StandaloneCacheFixture.ConnectionString);

    public async Task DisposeAsync() => await _foreign.DisposeAsync();

    [Fact]
    public async Task StressNoStaleAfterQuiescence()
    {
        var keys = StressHarness.KeyPool("stress-foreign", 20);

        // Seed, so that every read is of an existing key and a stale L1 entry is a value mismatch rather
        // than "L1 has something, Redis has nothing".
        foreach (var key in keys) await _foreign.Db.StringSetAsync(key, "0");

        var cliWrites = 0;
        long writeIndex = 0;
        var outcome = await StressHarness.RunAsync(
            _fx.Cache,
            keys,
            readerCount: 32,
            duration: TimeSpan.FromSeconds(3),
            writeAsync: async (key, value) =>
            {
                // Mostly through a foreign multiplexer (thousands of writes/s); every 64th write goes through
                // `docker exec redis-cli` so the test also covers an out-of-process client, which is the
                // scenario the library actually claims to handle.
                if (Interlocked.Increment(ref writeIndex) % 64 == 0)
                {
                    await Task.Run(() => RedisCli.Standalone("SET", key, value));
                    cliWrites++;
                }
                else
                {
                    await _foreign.Db.StringSetAsync(key, value);
                }
            });

        Assert.True(outcome.ReaderFailures.IsEmpty,
            "readers threw: " + string.Join(" | ", outcome.ReaderFailures.Select(e => e.ToString()).Take(3)));
        Assert.True(outcome.Writes > 100, $"writer only managed {outcome.Writes} writes; the window was too quiet to be a stress test.");
        Assert.True(outcome.Reads > 1_000, $"readers only managed {outcome.Reads} reads; the window was too quiet to be a stress test.");

        var settled = await ChaosSupport.QuiesceAsync(_fx.Cache, TimeSpan.FromSeconds(1));
        Assert.True(settled, "invalidations were still arriving after the settle deadline; the system never quiesced.");

        // Redis is the ground truth, but also check it against what the writer believes it wrote last: if
        // these disagree the test itself is wrong, not the cache.
        foreach (var (key, expected) in outcome.FinalValues)
        {
            var actual = await _foreign.Db.StringGetAsync(key);
            Assert.Equal(expected, actual.ToString());
        }

        var stale = await ChaosSupport.FindStaleAsync(_fx.Cache, _foreign, keys);
        _out.WriteLine($"reads={outcome.Reads} writes={outcome.Writes} (of which redis-cli={cliWrites}) stats={_fx.Cache.Statistics}");
        Assert.True(stale.Count == 0,
            "KNOWN DEFECT: L1 served a stale value after quiescence: " + string.Join(" | ", stale) +
            ". The invalidation was received and counted but dropped, because RedisNearCache.cs:71-72 evicts " +
            "L1 before marking the in-flight tracker. See the remarks on this class.");
    }
}
