using System.Diagnostics.Metrics;
using RedisNearCache.Caching;
using Xunit;

namespace RedisNearCache.UnitTests;

/// <summary>
/// The tag set on every measurement, which is what an existing dashboard or alert is written against. A cache that
/// was not registered under a name must publish EXACTLY the one tag it always published
/// (<c>rnc.client_name</c>): a second tag, even one whose value is null or empty, changes the series identity and
/// silently splits every existing chart. A named one adds <c>rnc.instance</c> and nothing else.
/// </summary>
public class NamedInstanceMetricsTagTests
{
    /// <summary>
    /// The tag sets of every measurement published for one client name, one entry per instrument. Filtered on the
    /// client name because other test classes run in parallel in the same process and publish to the same meter.
    /// </summary>
    private static List<KeyValuePair<string, object?>[]> TagSetsFor(string clientName)
    {
        var tagSets = new List<KeyValuePair<string, object?>[]>();
        using var listener = new MeterListener();
        listener.InstrumentPublished = (instrument, l) =>
        {
            if (instrument.Meter.Name == RedisNearCacheStatistics.MeterName) l.EnableMeasurementEvents(instrument);
        };
        listener.SetMeasurementEventCallback<long>((_, _, tags, _) =>
        {
            var copy = tags.ToArray();
            if (copy.Any(t => t.Key == RedisNearCacheMetrics.ClientNameTag && (string?)t.Value == clientName)) tagSets.Add(copy);
        });
        listener.Start();
        listener.RecordObservableInstruments();
        return tagSets;
    }

    private static string[] Keys(KeyValuePair<string, object?>[] tags) =>
        tags.Select(t => t.Key).OrderBy(k => k, StringComparer.Ordinal).ToArray();

    [Fact]
    public void WithoutAnInstanceNameTheOnlyTagIsTheClientName()
    {
        var clientName = "rnc-tags-default-" + Guid.NewGuid().ToString("N");
        using var metrics = new RedisNearCacheMetrics(new RedisNearCacheStatistics(), clientName, () => true);

        var tagSets = TagSetsFor(clientName);

        Assert.NotEmpty(tagSets);
        foreach (var tags in tagSets)
        {
            Assert.Equal(new[] { RedisNearCacheMetrics.ClientNameTag }, Keys(tags));
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
        foreach (var tags in tagSets)
        {
            Assert.Equal(
                new[] { RedisNearCacheMetrics.ClientNameTag, RedisNearCacheMetrics.InstanceTag }.OrderBy(k => k, StringComparer.Ordinal).ToArray(),
                Keys(tags));
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
        foreach (var tags in tagSets)
        {
            Assert.Equal(new[] { RedisNearCacheMetrics.ClientNameTag }, Keys(tags));
        }
    }

    /// <summary>The tag name itself is part of the contract, so it is pinned as a literal here rather than by reference.</summary>
    [Fact]
    public void TheTagNamesAreTheDocumentedOnes()
    {
        Assert.Equal("rnc.client_name", RedisNearCacheMetrics.ClientNameTag);
        Assert.Equal("rnc.instance", RedisNearCacheMetrics.InstanceTag);
    }
}
