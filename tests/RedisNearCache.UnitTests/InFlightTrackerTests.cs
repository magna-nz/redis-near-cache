using RedisNearCache.Caching;
using Xunit;

namespace RedisNearCache.UnitTests;

public class InFlightTrackerTests
{
    [Fact]
    public void InvalidationDuringFlightIsSeen()
    {
        var t = new InFlightTracker();
        var token = t.Begin("k");
        t.MarkInvalidated("k");
        Assert.True(t.WasInvalidated("k", token));
        t.End("k", token);
    }

    [Fact]
    public void InvalidationBeforeBeginIsNotSeen()
    {
        var t = new InFlightTracker();
        t.MarkInvalidated("k"); // nothing in flight: dropped by design (the GET has not been sent yet)
        var token = t.Begin("k");
        Assert.False(t.WasInvalidated("k", token));
        t.End("k", token);
    }

    [Fact]
    public void OtherKeysAreUnaffected()
    {
        var t = new InFlightTracker();
        var a = t.Begin("a");
        var b = t.Begin("b");
        t.MarkInvalidated("a");
        Assert.True(t.WasInvalidated("a", a));
        Assert.False(t.WasInvalidated("b", b));
        t.End("a", a); t.End("b", b);
    }

    [Fact]
    public void ConcurrentReadersOfSameKeyAllSeeInvalidation()
    {
        var t = new InFlightTracker();
        var first = t.Begin("k");
        var second = t.Begin("k");
        t.MarkInvalidated("k");
        Assert.True(t.WasInvalidated("k", first));
        Assert.True(t.WasInvalidated("k", second));
        t.End("k", first);
        // The entry must survive until the last reader ends.
        Assert.True(t.WasInvalidated("k", second));
        t.End("k", second);
    }

    [Fact]
    public void EntryIsForgottenAfterLastEnd()
    {
        var t = new InFlightTracker();
        var token = t.Begin("k");
        t.MarkInvalidated("k");
        t.End("k", token);
        // A later read starts clean: the old invalidation must not leak into it.
        var next = t.Begin("k");
        Assert.False(t.WasInvalidated("k", next));
        t.End("k", next);
    }

    [Fact]
    public void MarkAllInvalidatesEveryInFlightRead()
    {
        var t = new InFlightTracker();
        var a = t.Begin("a");
        var b = t.Begin("b");
        t.MarkAllInvalidated();
        Assert.True(t.WasInvalidated("a", a));
        Assert.True(t.WasInvalidated("b", b));
        var c = t.Begin("c"); // began after the flush: not affected
        Assert.False(t.WasInvalidated("c", c));
        t.End("a", a); t.End("b", b); t.End("c", c);
    }

    [Fact]
    public void ManyConcurrentBeginEndCyclesDoNotThrow()
    {
        var t = new InFlightTracker();
        Parallel.For(0, 10_000, i =>
        {
            var key = "k" + (i % 8);
            var token = t.Begin(key);
            if (i % 3 == 0) t.MarkInvalidated(key);
            _ = t.WasInvalidated(key, token);
            t.End(key, token);
        });
        var token2 = t.Begin("k0");
        Assert.False(t.WasInvalidated("k0", token2));
        t.End("k0", token2);
    }
}
