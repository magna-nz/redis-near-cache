using System.Diagnostics.Metrics;
using Xunit;
using Xunit.Abstractions;

namespace RedisNearCache.Tests.EdgeCases;

/// <summary>
/// The observable instruments published under <see cref="RedisNearCacheStatistics.MeterName"/>, driven by real
/// reads and a real invalidation. Every measurement is filtered on this instance's <c>rnc.client_name</c> tag:
/// other test classes run in parallel in the same process and publish to a meter of the same name.
/// </summary>
public class MetricsTests
{
    private const string ClientNameTag = "rnc.client_name";

    private readonly ITestOutputHelper _out;

    public MetricsTests(ITestOutputHelper output) => _out = output;

    /// <summary>One round of observable measurements for one cache instance, keyed by instrument name.</summary>
    private static Dictionary<string, long> Collect(string clientName)
    {
        var collected = new Dictionary<string, long>();
        using var listener = new MeterListener();
        listener.InstrumentPublished = (instrument, l) =>
        {
            if (instrument.Meter.Name == RedisNearCacheStatistics.MeterName) l.EnableMeasurementEvents(instrument);
        };
        listener.SetMeasurementEventCallback<long>((instrument, value, tags, _) =>
        {
            foreach (var tag in tags)
            {
                if (tag.Key == ClientNameTag && (string?)tag.Value == clientName) collected[instrument.Name] = value;
            }
        });
        listener.Start();
        listener.RecordObservableInstruments();
        return collected;
    }

    [Fact]
    public async Task InstrumentsFollowRealReadsAndAForeignWrite()
    {
        var handle = await EdgeCaseSupport.BuildAsync(StandaloneCacheFixture.ConnectionString);
        try
        {
            var cache = handle.Cache;
            var clientName = handle.Connection.ClientName;
            var key = TestHelpers.Key("metrics");

            var start = Collect(clientName);
            Assert.Equal(0, start["redisnearcache.hits"]);
            Assert.Equal(0, start["redisnearcache.misses"]);
            Assert.Equal(1, start["redisnearcache.coherent"]);
            Assert.Equal(0, start["redisnearcache.l1.entries"]);

            // A foreign write, so nothing this cache did can be confused with the invalidation asserted below.
            RedisCli.Standalone("SET", key, "v1");

            Assert.Equal("v1", await cache.GetAsync<string>(key)); // miss, stores
            Assert.True(await Poll.UntilAsync(() => cache.TryGetLocal<string>(key, out _)),
                "the first read did not populate L1, so the hit below would not be one.");
            Assert.Equal("v1", await cache.GetAsync<string>(key)); // hit

            var afterReads = Collect(clientName);
            _out.WriteLine($"after reads: {string.Join(", ", afterReads.Select(kv => $"{kv.Key}={kv.Value}"))}");
            Assert.Equal(1, afterReads["redisnearcache.hits"]);
            Assert.Equal(1, afterReads["redisnearcache.misses"]);
            Assert.Equal(1, afterReads["redisnearcache.l1.entries"]);
            Assert.Equal(1, afterReads["redisnearcache.coherent"]);
            Assert.Equal(0, afterReads["redisnearcache.invalidations"]);
            Assert.Equal(0, afterReads["redisnearcache.flushes"]);
            Assert.Equal(0, afterReads["redisnearcache.rearms"]);
            Assert.Equal(0, afterReads["redisnearcache.race_discards"]);
            Assert.Equal(cache.Statistics.L1Entries, afterReads["redisnearcache.l1.entries"]);

            // A foreign write to the tracked key: the server pushes an invalidation, which drops the entry.
            RedisCli.Standalone("SET", key, "v2");

            var invalidated = await Poll.UntilAsync(
                () => Collect(clientName) is { } m && m["redisnearcache.invalidations"] >= 1 && m["redisnearcache.l1.entries"] == 0,
                TimeSpan.FromSeconds(5));
            var final = Collect(clientName);
            _out.WriteLine($"after the foreign write: {string.Join(", ", final.Select(kv => $"{kv.Key}={kv.Value}"))}");
            Assert.True(invalidated,
                $"the invalidation was not reflected in the instruments within the deadline: {string.Join(", ", final.Select(kv => $"{kv.Key}={kv.Value}"))}");
            Assert.Equal(cache.Statistics.Invalidations, final["redisnearcache.invalidations"]);
            Assert.Equal(1, final["redisnearcache.coherent"]);
        }
        finally
        {
            await handle.DisposeAsync();
        }
    }

    [Fact]
    public async Task NothingIsPublishedForADisposedCache()
    {
        var handle = await EdgeCaseSupport.BuildAsync(StandaloneCacheFixture.ConnectionString);
        var clientName = handle.Connection.ClientName;
        await handle.Cache.GetAsync<string>(TestHelpers.Key("metrics-disposed"));
        Assert.NotEmpty(Collect(clientName));

        await handle.DisposeAsync();

        // The meter goes with the cache, and collecting across the teardown must not throw from the disposed L1.
        Assert.Empty(Collect(clientName));
    }
}
