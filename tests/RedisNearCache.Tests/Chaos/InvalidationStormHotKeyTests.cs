using System.Globalization;
using Xunit;
using Xunit.Abstractions;

namespace RedisNearCache.Tests.Chaos;

/// <summary>
/// One key, 500 foreign writes as fast as they will go, and a reader that never stops trying to cache it.
/// Tracking is one-shot per key (spike README), so the key oscillates between tracked and untracked hundreds
/// of times and the reader is re-arming that tracking from underneath the storm. Every read here is a
/// candidate for the in-flight race: the reply is already on the wire when the next write lands.
/// </summary>
/// <remarks>
/// Regression test for a race between an invalidation and a concurrent read of the same key: without the
/// fix below, L1 could be left holding a value from the middle of the storm, with Redis holding the last
/// one and no further invalidation ever arriving, because tracking is one-shot and the invalidation that
/// should have caught the stale store had already consumed it.
///
/// <c>OnKeyInvalidated</c> (src/RedisNearCache/Caching/RedisNearCache.cs) marks the in-flight tracker
/// FIRST, then evicts L1, per the ordering rule documented immediately above it:
///
///     _inflight.MarkInvalidated(key);
///     _l1.Remove(key);
///
/// A reader running concurrently in <c>GetAsync</c> re-checks <c>WasInvalidated</c> after storing into L1
/// and before ending its in-flight entry. Mark-then-evict closes the interleaving that evict-then-mark left
/// open: the mark now precedes the Remove, the Remove precedes the Set that would otherwise leave a stale
/// value behind, so the reader's re-check is guaranteed to see the mark once it has been set.
/// </remarks>
public class InvalidationStormHotKeyTests : IClassFixture<StandaloneCacheFixture>, IAsyncLifetime
{
    private readonly StandaloneCacheFixture _fx;
    private readonly ITestOutputHelper _out;
    private ForeignClient _foreign = null!;

    public InvalidationStormHotKeyTests(StandaloneCacheFixture fx, ITestOutputHelper output)
    {
        _fx = fx;
        _out = output;
    }

    public async Task InitializeAsync() =>
        _foreign = await ForeignClient.ConnectAsync(StandaloneCacheFixture.ConnectionString);

    public async Task DisposeAsync() => await _foreign.DisposeAsync();

    [Fact]
    public async Task InvalidationStormHotKey()
    {
        const int writes = 500;
        var key = TestHelpers.Key("storm");
        await _foreign.Db.StringSetAsync(key, "0");

        var invalidationsBefore = _fx.Cache.Statistics.Invalidations;
        var raceDiscardsBefore = _fx.Cache.Statistics.RaceDiscards;

        var failures = new List<Exception>();
        long reads = 0;
        using var stop = new CancellationTokenSource();
        var reader = Task.Run(async () =>
        {
            while (!stop.IsCancellationRequested)
            {
                try
                {
                    _ = await _fx.Cache.GetAsync<string>(key);
                    Interlocked.Increment(ref reads);
                }
                catch (Exception ex)
                {
                    lock (failures) failures.Add(ex);
                }

                await Task.Yield();
            }
        });

        string last = "0";
        for (var i = 1; i <= writes; i++)
        {
            last = i.ToString(CultureInfo.InvariantCulture);
            await _foreign.Db.StringSetAsync(key, last);
        }

        await stop.CancelAsync();
        await reader;

        lock (failures)
        {
            Assert.True(failures.Count == 0, "reader threw: " + string.Join(" | ", failures.Select(e => e.ToString()).Take(3)));
        }

        var settled = await ChaosSupport.QuiesceAsync(_fx.Cache, TimeSpan.FromSeconds(1));
        Assert.True(settled, "the system never quiesced after the storm.");

        var invalidations = _fx.Cache.Statistics.Invalidations - invalidationsBefore;
        var raceDiscards = _fx.Cache.Statistics.RaceDiscards - raceDiscardsBefore;
        _out.WriteLine($"{writes} writes, {reads} reads => invalidations={invalidations} raceDiscards={raceDiscards} " +
                       $"(far fewer invalidations than writes is expected: tracking is one-shot per key, so a write " +
                       $"only produces a message if the key had been re-read since the previous one). stats={_fx.Cache.Statistics}");

        var actual = await _foreign.Db.StringGetAsync(key);
        Assert.Equal(last, actual.ToString());

        if (_fx.Cache.TryGetLocal<string>(key, out var local))
        {
            Assert.True(
                local == last,
                $"L1 held '{local}' after the storm settled but Redis holds '{last}' " +
                $"(invalidations={invalidations}). See the remarks on this class for the race this guards against.");
        }

        Assert.True(invalidations >= 1, "the storm produced no invalidations at all, so the test proved nothing.");
    }
}
