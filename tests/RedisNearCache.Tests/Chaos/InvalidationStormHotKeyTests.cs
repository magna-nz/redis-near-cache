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
/// KNOWN DEFECT — this test fails most of the time (4 of 5 attempts while it was written), always with the
/// same signature: L1 is left holding some value from the middle of the storm, Redis holds the last one, and
/// no further invalidation ever arrives because the server stopped tracking the key at that write.
///
/// The cause is that <c>OnKeyInvalidated</c> (src/RedisNearCache/Caching/RedisNearCache.cs:69-74) evicts L1
/// and marks the in-flight tracker as two separate, unsynchronised steps, in that order:
///
///     _l1.Remove(key);                  // line 71
///     _inflight.MarkInvalidated(key);   // line 72
///
/// A reader running concurrently in GetAsync (same file, lines 163-179 plus the finally at 186) can slot in
/// between them:
///
///   1. invalidation thread   line 71   _l1.Remove(key)            -> L1 is empty, nothing to remove
///   2. invalidation thread   PRE-EMPTED between line 71 and 72
///   3. reader                line 165  WasInvalidated(key, token) -> false, the mark has not happened yet
///   4. reader                line 171  _l1.Set(key, staleBytes)
///   5. reader                line 174  WasInvalidated(key, token) -> still false
///   6. reader                line 186  _inflight.End(key, token)  -> the key's Entry is removed outright
///                                      (InFlightTracker.cs:57-73)
///   7. invalidation thread   line 72   MarkInvalidated(key)       -> TryGetValue finds no Entry
///                                      (InFlightTracker.cs:79-81) and returns; the mark is dropped
///
/// L1 now holds a value the server superseded, the server is no longer tracking the key (tracking is
/// one-shot, and the invalidation it just sent consumed it), so nothing will ever evict the entry. It is
/// served as a hit until L1MaxAge expires it — five minutes by default.
///
/// Steps 3 and 5 are what makes the ordering matter: both of the reader's checks are unavoidably before
/// step 7. Marking first and evicting second closes the window, because then the mark precedes the Remove,
/// the Remove precedes the Set (that is what leaves the value behind at all), and so the re-check at line
/// 174 is guaranteed to see it.
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
                $"KNOWN DEFECT: L1 held '{local}' after the storm settled but Redis holds '{last}'. The " +
                $"invalidation that should have evicted '{local}' was counted (invalidations={invalidations}) " +
                "but dropped by InFlightTracker.MarkInvalidated because the reader had already ended its " +
                "in-flight entry. See the remarks on this class: RedisNearCache.cs:71-72 evicts L1 before " +
                "marking the tracker, and the two steps are not atomic with respect to a concurrent GetAsync.");
        }

        Assert.True(invalidations >= 1, "the storm produced no invalidations at all, so the test proved nothing.");
    }
}
