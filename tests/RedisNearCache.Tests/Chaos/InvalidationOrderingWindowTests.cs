using System.Text;
using RedisNearCache.Caching;
using Xunit;
using Xunit.Abstractions;

namespace RedisNearCache.Tests.Chaos;

/// <summary>
/// <see cref="InvalidationStormHotKeyTests"/> proves a stale entry exists but only wins a race about four
/// times in five. This pins the same defect deterministically.
/// <para>
/// It is a <b>scheduling model</b>, not a test of the facade: it drives the real internal
/// <see cref="L1Cache"/> and <see cref="InFlightTracker"/> through the exact statement sequence that
/// <c>RedisNearCache.OnKeyInvalidated</c> and <c>RedisNearCache.GetAsync</c> execute, in the one interleaving
/// the storm test hits by luck. No threads are needed, because writing the interleaving out by hand IS the
/// interleaving. What it therefore shows is a property of the ordering of those statements, which is what the
/// fix has to change; it deliberately does not go through <see cref="IRedisNearCache"/>, so it will keep
/// passing after src/ is fixed and is not a regression guard.
/// </para>
/// <para>
/// Line references (all src/RedisNearCache/Caching/):
/// RedisNearCache.cs:71 <c>_l1.Remove(key)</c>, :72 <c>_inflight.MarkInvalidated(key)</c>,
/// :145 <c>Begin</c>, :165 first <c>WasInvalidated</c>, :171 <c>_l1.Set</c>, :174 re-check, :186 <c>End</c>;
/// InFlightTracker.cs:57-73 <c>End</c> removes the entry, :79-81 <c>MarkInvalidated</c> returns early when
/// there is no entry.
/// </para>
/// </summary>
public class InvalidationOrderingWindowTests
{
    private readonly ITestOutputHelper _out;

    public InvalidationOrderingWindowTests(ITestOutputHelper output) => _out = output;

    private static L1Cache NewL1() =>
        new(new RedisNearCacheOptions { L1MaxAge = Timeout.InfiniteTimeSpan, L1SizeLimit = 100 });

    [Fact]
    public void EvictBeforeMarkLosesTheInvalidation()
    {
        const string key = "ordering:window";
        var stale = Encoding.UTF8.GetBytes("stale");

        using var l1 = NewL1();
        var inflight = new InFlightTracker();

        // A read is in flight: GetAsync:145 has run and the Redis reply ("stale") has arrived, but the store
        // has not happened yet.
        var token = inflight.Begin(key);

        // The invalidation handler starts. RedisNearCache.cs:71.
        l1.Remove(key);
        // ... and is pre-empted before RedisNearCache.cs:72.

        // The reader completes its store. GetAsync:163-179.
        Assert.False(inflight.WasInvalidated(key, token));   // :165
        l1.Set(key, stale);                                  // :171
        Assert.False(inflight.WasInvalidated(key, token));   // :174 - the documented re-check
        inflight.End(key, token);                            // :186 - drops the key's Entry entirely

        // The invalidation handler resumes. RedisNearCache.cs:72.
        inflight.MarkInvalidated(key);                       // no Entry left: InFlightTracker.cs:79-81 returns

        var survived = l1.TryGet(key, out var value);
        _out.WriteLine($"evict-then-mark: L1 {(survived ? $"still holds '{Encoding.UTF8.GetString(value!)}'" : "is empty")}");

        Assert.True(survived,
            "this models the defect; if L1 is empty here the ordering hazard is gone and this test should be deleted.");
    }

    [Fact]
    public void MarkBeforeEvictSurvivesTheSameInterleaving()
    {
        const string key = "ordering:window";
        var stale = Encoding.UTF8.GetBytes("stale");

        using var l1 = NewL1();
        var inflight = new InFlightTracker();

        var token = inflight.Begin(key);

        // The proposed order: mark first, evict second.
        inflight.MarkInvalidated(key);
        // ... pre-empted at the same point, between the two statements.

        var discarded = false;
        if (inflight.WasInvalidated(key, token))             // :165 - now true
        {
            discarded = true;
        }
        else
        {
            l1.Set(key, stale);                              // :171
            if (inflight.WasInvalidated(key, token))         // :174
            {
                l1.Remove(key);
                discarded = true;
            }
        }

        inflight.End(key, token);
        l1.Remove(key);                                      // the handler resumes

        _out.WriteLine($"mark-then-evict: discarded={discarded}, L1 empty={!l1.TryGet(key, out _)}");

        Assert.True(discarded, "the reader should have discarded the stale reply.");
        Assert.False(l1.TryGet(key, out _), "no stale value should survive.");
    }

    /// <summary>
    /// The second, narrower window in the same pair of methods, kept separate because swapping the two lines
    /// does NOT close it: <see cref="InFlightTracker.Begin"/> takes its token (InFlightTracker.cs:39) before
    /// it registers the entry (:43), so a mark landing in between is dropped too. It is benign for external
    /// writes — the GET has not been sent yet at that point, so the reply cannot predate the write — but it
    /// is the reason a fix should make Begin's increment and registration atomic rather than only reorder
    /// the two calls in OnKeyInvalidated.
    /// </summary>
    [Fact]
    public void MarkBetweenTokenAndRegistrationIsAlsoDropped()
    {
        const string key = "ordering:begin-window";

        var inflight = new InFlightTracker();

        // Nothing in flight: this models the mark arriving in Begin's own window, which behaves identically
        // to it arriving before Begin was called at all.
        inflight.MarkInvalidated(key);
        var token = inflight.Begin(key);

        Assert.False(inflight.WasInvalidated(key, token),
            "a mark taken while no entry is registered leaves no trace for the read that registers next.");
        inflight.End(key, token);
    }
}
