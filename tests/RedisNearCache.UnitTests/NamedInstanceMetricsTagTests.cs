using System.Diagnostics.Metrics;
using RedisNearCache.Caching;
using Xunit;

namespace RedisNearCache.UnitTests;

/// <summary>
/// The tag set on every measurement, which is what an existing dashboard or alert is written against. A cache that
/// was not registered under a name must publish EXACTLY the one instance tag it always published
/// (<c>rnc.client_name</c>): a second tag, even one whose value is null or empty, changes the series identity and
/// silently splits every existing chart. A named one adds <c>rnc.instance</c> and nothing else.
/// </summary>
/// <remarks>
/// The two per-reason breakdowns carry one further tag, <c>reason</c>, which is the point of them; it is asserted
/// here to appear on exactly those two instruments and on no other, and is excluded from the instance-tag comparison
/// rather than being allowed to weaken it.
/// </remarks>
public class NamedInstanceMetricsTagTests
{
    /// <summary>The only instruments allowed to carry the <c>reason</c> tag.</summary>
    private static readonly string[] ReasonTagged = ["redisnearcache.rearms", "redisnearcache.flushes"];

    /// <summary>
    /// The tag sets of every measurement published for one client name, with the instrument that published each.
    /// Filtered on the client name because other test classes run in parallel in the same process and publish to the
    /// same meter. Both value types are collected: the pass-through gauge is the one <c>double</c> instrument, and its
    /// tags are part of the same contract.
    /// </summary>
    private static List<(string Instrument, KeyValuePair<string, object?>[] Tags)> TagSetsFor(string clientName)
    {
        var tagSets = new List<(string, KeyValuePair<string, object?>[])>();
        using var listener = new MeterListener();
        listener.InstrumentPublished = (instrument, l) =>
        {
            if (instrument.Meter.Name == RedisNearCacheStatistics.MeterName) l.EnableMeasurementEvents(instrument);
        };
        listener.SetMeasurementEventCallback<long>((instrument, _, tags, _) => Record(instrument.Name, tags));
        listener.SetMeasurementEventCallback<double>((instrument, _, tags, _) => Record(instrument.Name, tags));
        listener.Start();
        listener.RecordObservableInstruments();
        return tagSets;

        void Record(string name, ReadOnlySpan<KeyValuePair<string, object?>> tags)
        {
            var copy = tags.ToArray();
            if (copy.Any(t => t.Key == RedisNearCacheMetrics.ClientNameTag && (string?)t.Value == clientName)) tagSets.Add((name, copy));
        }
    }

    /// <summary>
    /// The instance tag keys of one measurement, i.e. everything but the per-reason breakdown's own tag - which is
    /// asserted here to be present on exactly the two instruments that are supposed to have it.
    /// </summary>
    private static string[] InstanceKeys(string instrument, KeyValuePair<string, object?>[] tags)
    {
        var reasons = tags.Count(t => t.Key == RedisNearCacheMetrics.ReasonTag);
        Assert.Equal(ReasonTagged.Contains(instrument) ? 1 : 0, reasons);
        return tags.Where(t => t.Key != RedisNearCacheMetrics.ReasonTag)
            .Select(t => t.Key).OrderBy(k => k, StringComparer.Ordinal).ToArray();
    }

    [Fact]
    public void WithoutAnInstanceNameTheOnlyTagIsTheClientName()
    {
        var clientName = "rnc-tags-default-" + Guid.NewGuid().ToString("N");
        using var metrics = new RedisNearCacheMetrics(new RedisNearCacheStatistics(), clientName, () => true);

        var tagSets = TagSetsFor(clientName);

        Assert.NotEmpty(tagSets);
        foreach (var (instrument, tags) in tagSets)
        {
            Assert.Equal(new[] { RedisNearCacheMetrics.ClientNameTag }, InstanceKeys(instrument, tags));
            // The instance tags come first, so the client name is still tags[0] on a reason-tagged measurement too.
            Assert.Equal(clientName, (string?)tags[0].Value);
        }
    }

    [Fact]
    public void AnInstanceNameAddsExactlyTheInstanceTag()
    {
        var clientName = "rnc-tags-named-" + Guid.NewGuid().ToString("N");
        using var metrics = new RedisNearCacheMetrics(new RedisNearCacheStatistics(), clientName, () => true, "a");

        var tagSets = TagSetsFor(clientName);

        Assert.NotEmpty(tagSets);
        foreach (var (instrument, tags) in tagSets)
        {
            Assert.Equal(
                new[] { RedisNearCacheMetrics.ClientNameTag, RedisNearCacheMetrics.InstanceTag }.OrderBy(k => k, StringComparer.Ordinal).ToArray(),
                InstanceKeys(instrument, tags));
            Assert.Equal("a", (string?)tags.Single(t => t.Key == RedisNearCacheMetrics.InstanceTag).Value);
            Assert.Equal(clientName, (string?)tags.Single(t => t.Key == RedisNearCacheMetrics.ClientNameTag).Value);
        }
    }

    /// <summary>
    /// Explicitly passing null is the same as not passing it: the optional parameter is what every existing call
    /// site (the default registration) now goes through, so the two must be indistinguishable.
    /// </summary>
    [Fact]
    public void AnExplicitlyNullInstanceNameIsTheSameAsNone()
    {
        var clientName = "rnc-tags-null-" + Guid.NewGuid().ToString("N");
        using var metrics = new RedisNearCacheMetrics(new RedisNearCacheStatistics(), clientName, () => true, null);

        var tagSets = TagSetsFor(clientName);

        Assert.NotEmpty(tagSets);
        foreach (var (instrument, tags) in tagSets)
        {
            Assert.Equal(new[] { RedisNearCacheMetrics.ClientNameTag }, InstanceKeys(instrument, tags));
        }
    }

    /// <summary>Every reason-tagged instrument publishes one measurement per reason, all with the same instance tags.</summary>
    [Fact]
    public void TheReasonTagAppearsOnlyOnTheTwoBreakdownsAndOnEveryOneOfTheirMeasurements()
    {
        var clientName = "rnc-tags-reason-" + Guid.NewGuid().ToString("N");
        using var metrics = new RedisNearCacheMetrics(new RedisNearCacheStatistics(), clientName, () => true);

        var tagSets = TagSetsFor(clientName);

        foreach (var instrument in ReasonTagged)
        {
            var measurements = tagSets.Where(t => t.Instrument == instrument).ToArray();
            Assert.True(measurements.Length > 1, $"{instrument} must publish one measurement per reason, saw {measurements.Length}");
            var reasons = measurements
                .Select(m => (string?)m.Tags.Single(t => t.Key == RedisNearCacheMetrics.ReasonTag).Value)
                .ToArray();
            Assert.Equal(reasons.Length, reasons.Distinct(StringComparer.Ordinal).Count());
            Assert.All(reasons, reason => Assert.False(string.IsNullOrEmpty(reason), "a reason tag with no value"));
        }

        // Nothing else carries it.
        foreach (var (instrument, tags) in tagSets.Where(t => !ReasonTagged.Contains(t.Instrument)))
        {
            Assert.False(tags.Any(t => t.Key == RedisNearCacheMetrics.ReasonTag), $"{instrument} must not carry a reason tag");
        }
    }

    /// <summary>The tag name itself is part of the contract, so it is pinned as a literal here rather than by reference.</summary>
    [Fact]
    public void TheTagNamesAreTheDocumentedOnes()
    {
        Assert.Equal("rnc.client_name", RedisNearCacheMetrics.ClientNameTag);
        Assert.Equal("rnc.instance", RedisNearCacheMetrics.InstanceTag);
        Assert.Equal("reason", RedisNearCacheMetrics.ReasonTag);
    }
}
