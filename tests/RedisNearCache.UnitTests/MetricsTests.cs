using System.Diagnostics.Metrics;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using RedisNearCache.Tracking;
using Xunit;
using Facade = RedisNearCache.Caching.RedisNearCache;

namespace RedisNearCache.UnitTests;

/// <summary>
/// The observable instruments published under <see cref="RedisNearCacheStatistics.MeterName"/>. Every assertion
/// filters on this instance's <c>rnc.client_name</c> tag: other test classes run in parallel in the same process
/// and publish to a meter of the same name.
/// </summary>
public class MetricsTests
{
    private const string ClientNameTag = "rnc.client_name";

    private static readonly string[] ExpectedInstruments =
    [
        "redisnearcache.hits",
        "redisnearcache.misses",
        "redisnearcache.invalidations",
        "redisnearcache.flushes",
        "redisnearcache.rearms",
        "redisnearcache.race_discards",
        "redisnearcache.l1.entries",
        "redisnearcache.coherent",
    ];

    private static async Task<Facade> StartAsync(string clientName)
    {
        var mux = new FakeMultiplexer(clientName);
        mux.Add(7000, isReplica: false);
        var connection = FakeRedis.Connection(mux);
        var armer = new TrackingArmer(connection, NullLogger<TrackingArmer>.Instance, Timeout.InfiniteTimeSpan);
        var cache = new Facade(connection, armer, new SilentListener(), Options.Create(new RedisNearCacheOptions()), NullLogger<Facade>.Instance);
        await cache.Ready;
        return cache;
    }

    /// <summary>Collects one round of observable measurements, keyed by instrument name, for one client name only.</summary>
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
    public async Task EveryInstrumentIsPublishedAndMatchesTheStatistics()
    {
        var clientName = "rnc-metrics-" + Guid.NewGuid().ToString("N");
        await using var cache = await StartAsync(clientName);

        await cache.GetAsync<string>("k"); // miss, stores
        await cache.GetAsync<string>("k"); // hit
        cache.EvictAllLocal();             // flush
        await cache.GetAsync<string>("k"); // miss, stores again

        var measurements = Collect(clientName);

        Assert.Equal(ExpectedInstruments.OrderBy(n => n), measurements.Keys.OrderBy(n => n));
        Assert.Equal(cache.Statistics.Hits, measurements["redisnearcache.hits"]);
        Assert.Equal(cache.Statistics.Misses, measurements["redisnearcache.misses"]);
        Assert.Equal(cache.Statistics.Invalidations, measurements["redisnearcache.invalidations"]);
        Assert.Equal(cache.Statistics.Flushes, measurements["redisnearcache.flushes"]);
        Assert.Equal(cache.Statistics.Rearms, measurements["redisnearcache.rearms"]);
        Assert.Equal(cache.Statistics.RaceDiscards, measurements["redisnearcache.race_discards"]);
        Assert.Equal(cache.Statistics.L1Entries, measurements["redisnearcache.l1.entries"]);
        Assert.Equal(1, measurements["redisnearcache.hits"]);
        Assert.Equal(1, measurements["redisnearcache.flushes"]);
        Assert.Equal(1, measurements["redisnearcache.l1.entries"]);
        Assert.Equal(1, measurements["redisnearcache.coherent"]);
    }

    [Fact]
    public async Task TwoCachesInOneProcessAreToldApartByTheClientNameTag()
    {
        var first = "rnc-metrics-a-" + Guid.NewGuid().ToString("N");
        var second = "rnc-metrics-b-" + Guid.NewGuid().ToString("N");
        await using var a = await StartAsync(first);
        await using var b = await StartAsync(second);

        await a.GetAsync<string>("k");
        await a.GetAsync<string>("k");
        await a.GetAsync<string>("k"); // a: 1 miss, 2 hits

        var names = new HashSet<string>();
        long hitsOfA = -1, hitsOfB = -1;
        using var listener = new MeterListener();
        listener.InstrumentPublished = (instrument, l) =>
        {
            if (instrument.Meter.Name == RedisNearCacheStatistics.MeterName) l.EnableMeasurementEvents(instrument);
        };
        listener.SetMeasurementEventCallback<long>((instrument, value, tags, _) =>
        {
            foreach (var tag in tags)
            {
                if (tag.Key != ClientNameTag) continue;
                var name = (string?)tag.Value;
                if (name is null) continue;
                names.Add(name);
                if (instrument.Name != "redisnearcache.hits") continue;
                if (name == first) hitsOfA = value;
                if (name == second) hitsOfB = value;
            }
        });
        listener.Start();
        listener.RecordObservableInstruments();

        Assert.Contains(first, names);
        Assert.Contains(second, names);
        Assert.Equal(2, hitsOfA);
        Assert.Equal(0, hitsOfB);
    }

    [Fact]
    public async Task RecordingAfterDisposeDoesNotThrow()
    {
        var clientName = "rnc-metrics-disposed-" + Guid.NewGuid().ToString("N");
        var cache = await StartAsync(clientName);
        await cache.GetAsync<string>("k");
        Assert.NotEmpty(Collect(clientName));

        await cache.DisposeAsync();

        // The meter is disposed with the cache, so nothing is published for it any more - and asking for a round of
        // measurements while it goes away must not surface an ObjectDisposedException from the L1 store.
        var after = Collect(clientName);
        Assert.Empty(after);
    }

    [Fact]
    public async Task CoherentIsOneWhileArmedAndUnpublishedOnceTheCacheIsGone()
    {
        var clientName = "rnc-metrics-coherent-" + Guid.NewGuid().ToString("N");
        var cache = await StartAsync(clientName);
        Assert.Equal(1, Collect(clientName)["redisnearcache.coherent"]);
        Assert.True(cache.IsCoherent);

        await cache.DisposeAsync();

        Assert.False(cache.IsCoherent);
        // The meter went with the cache, so the gauge is no longer published at all rather than stuck at 1.
        Assert.False(Collect(clientName).ContainsKey("redisnearcache.coherent"));
    }
}
