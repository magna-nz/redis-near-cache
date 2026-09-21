using System.Diagnostics.Metrics;
using Microsoft.Extensions.DependencyInjection;
using RedisNearCache.Internal;
using Xunit;

namespace RedisNearCache.Tests.NamedInstances;

/// <summary>
/// The metrics a named instance publishes, end to end over real Redis: its measurements carry an extra
/// <c>rnc.instance</c> tag naming the registration, and the DEFAULT instance's carry exactly the tags they always
/// did. The second half is the compatibility guard - an existing dashboard groups by <c>rnc.client_name</c> and
/// must not suddenly see a second dimension on the series it was already charting.
/// </summary>
public class NamedInstanceMetricsTests
{
    private const string ClientNameTag = "rnc.client_name";
    private const string InstanceTag = "rnc.instance";
    private const string ReasonTag = "reason";

    /// <summary>The tag set of every measurement published for one client name, one per instrument.</summary>
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
            if (copy.Any(t => t.Key == ClientNameTag && (string?)t.Value == clientName)) tagSets.Add(copy);
        });
        listener.Start();
        listener.RecordObservableInstruments();
        return tagSets;
    }

    [Fact]
    public async Task ANamedInstanceIsTaggedWithItsNameAndTheDefaultOneIsNot()
    {
        var services = new ServiceCollection();
        services.AddRedisNearCache(StandaloneCacheFixture.ConnectionString);
        services.AddKeyedRedisNearCache("a", StandaloneCacheFixture.ConnectionString);
        var provider = services.BuildServiceProvider();
        var namedKey = TestHelpers.Key("metrics-named");
        var defaultKey = TestHelpers.Key("metrics-default");
        try
        {
            var named = provider.GetRequiredKeyedService<IRedisNearCache>("a");
            var @default = provider.GetRequiredService<IRedisNearCache>();
            await Task.WhenAll(named.Ready, @default.Ready);
            var namedClient = provider.GetRequiredKeyedService<RedisNearCacheConnection>("a").ClientName;
            var defaultClient = provider.GetRequiredService<RedisNearCacheConnection>().ClientName;
            Assert.NotEqual(namedClient, defaultClient);

            // Real work, so the measurements are not all zero and the two instances are told apart by value too.
            RedisCli.Standalone("SET", namedKey, "v1");
            RedisCli.Standalone("SET", defaultKey, "v1");
            Assert.True(await TestHelpers.ReadUntilCachedAsync(named, namedKey, "v1"));
            Assert.Equal("v1", await named.GetAsync<string>(namedKey)); // a hit, so hits == 1 for the named one only
            Assert.True(await TestHelpers.ReadUntilCachedAsync(@default, defaultKey, "v1"));

            var namedTagSets = TagSetsFor(namedClient);
            var defaultTagSets = TagSetsFor(defaultClient);

            Assert.NotEmpty(namedTagSets);
            Assert.NotEmpty(defaultTagSets);

            // The INSTANCE-identifying tags are what this test is about, so the per-reason breakdown tag carried by
            // redisnearcache.flushes and redisnearcache.rearms is excluded from the comparison: it varies per
            // measurement, not per instance. That it appears at all, and only on those two, is asserted by the unit
            // twin (NamedInstanceMetricsTagTests).
            foreach (var tags in namedTagSets)
            {
                Assert.Equal(
                    new[] { ClientNameTag, InstanceTag },
                    tags.Select(t => t.Key).Where(k => k != ReasonTag).OrderBy(k => k, StringComparer.Ordinal).ToArray());
                Assert.Equal("a", (string?)tags.Single(t => t.Key == InstanceTag).Value);
            }

            foreach (var tags in defaultTagSets)
            {
                Assert.Equal(new[] { ClientNameTag }, tags.Select(t => t.Key).Where(k => k != ReasonTag).ToArray());
                Assert.DoesNotContain(tags, t => t.Key == InstanceTag);
            }
        }
        finally
        {
            await provider.DisposeAsync();
            RedisCli.Standalone("DEL", namedKey, defaultKey);
        }
    }
}
