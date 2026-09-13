using Xunit;
using Xunit.Abstractions;

namespace RedisNearCache.Tests.Chaos;

/// <summary>
/// The same stress as <see cref="StressNoStaleAfterQuiescenceTests"/> but the writer is the cache's own
/// <see cref="IRedisNearCache.SetAsync{T}"/>. That is the NOLOOP path: the server deliberately does NOT echo
/// an invalidation back for our own writes, so coherence rests entirely on SetAsync marking the key in the
/// in-flight tracker and evicting L1 on both sides of the write
/// (src/RedisNearCache/Caching/RedisNearCache.cs:197-201). A concurrent reader whose GET passed the SET on
/// the wire is the interesting interleaving.
/// </summary>
/// <remarks>
/// This one has not been seen to fail, and the contrast with <see cref="StressNoStaleAfterQuiescenceTests"/>
/// is itself the evidence for where that defect is. <c>SetAsync</c> already does the two steps in the safe
/// order — <c>MarkInvalidated</c> at :197 and :200, <i>then</i> <c>_l1.Remove</c> at :198 and :201 — whereas
/// <c>OnKeyInvalidated</c> at :71-72 does them the other way round. Identical stress, identical assertion,
/// and only the path through the incorrectly ordered pair goes stale.
/// </remarks>
public class StressOwnWritesNoStaleTests : IClassFixture<StandaloneCacheFixture>, IAsyncLifetime
{
    private readonly StandaloneCacheFixture _fx;
    private readonly ITestOutputHelper _out;
    private ForeignClient _truth = null!;

    public StressOwnWritesNoStaleTests(StandaloneCacheFixture fx, ITestOutputHelper output)
    {
        _fx = fx;
        _out = output;
    }

    public async Task InitializeAsync() =>
        _truth = await ForeignClient.ConnectAsync(StandaloneCacheFixture.ConnectionString);

    public async Task DisposeAsync() => await _truth.DisposeAsync();

    [Fact]
    public async Task StressOwnWritesNoStale()
    {
        var keys = StressHarness.KeyPool("stress-own", 20);
        foreach (var key in keys) await _truth.Db.StringSetAsync(key, "0");

        var outcome = await StressHarness.RunAsync(
            _fx.Cache,
            keys,
            readerCount: 32,
            duration: TimeSpan.FromSeconds(3),
            writeAsync: async (key, value) => await _fx.Cache.SetAsync(key, value));

        Assert.True(outcome.ReaderFailures.IsEmpty,
            "readers threw: " + string.Join(" | ", outcome.ReaderFailures.Select(e => e.ToString()).Take(3)));
        Assert.True(outcome.Writes > 100, $"writer only managed {outcome.Writes} writes.");
        Assert.True(outcome.Reads > 1_000, $"readers only managed {outcome.Reads} reads.");

        var settled = await ChaosSupport.QuiesceAsync(_fx.Cache, TimeSpan.FromSeconds(1));
        Assert.True(settled, "the system never quiesced.");

        foreach (var (key, expected) in outcome.FinalValues)
        {
            var actual = await _truth.Db.StringGetAsync(key);
            Assert.Equal(expected, actual.ToString());
        }

        var stale = await ChaosSupport.FindStaleAsync(_fx.Cache, _truth, keys);
        _out.WriteLine($"reads={outcome.Reads} writes={outcome.Writes} stats={_fx.Cache.Statistics}");
        Assert.True(stale.Count == 0, "L1 served a stale value after quiescence (own-write/NOLOOP path): " + string.Join(" | ", stale));
    }
}
