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
        "redisnearcache.serializer_failures",
        "redisnearcache.l1.store_refusals",
        "redisnearcache.prearm_failures",
        "redisnearcache.l1.entries",
        "redisnearcache.coherent",
        "redisnearcache.pass_through.seconds",
        "redisnearcache.endpoints.lost",
        "redisnearcache.ttl_cap.abandoned",
        "redisnearcache.untracked_reads.unavailable",
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

    /// <summary>
    /// Collects one round of observable measurements for one client name only. The per-reason breakdowns make this
    /// more than a name-to-value map now, so it lives in <see cref="MeterProbe"/> and is shared with the
    /// observability tests; <c>Longs</c> sums over the tag sets, so a tagged instrument still reads as its total here.
    /// </summary>
    private static MeterSnapshot Collect(string clientName) => MeterProbe.Collect(clientName);

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

        Assert.Equal(ExpectedInstruments.OrderBy(n => n), measurements.Names.OrderBy(n => n));
        Assert.Equal(cache.Statistics.Hits, measurements.Longs["redisnearcache.hits"]);
        Assert.Equal(cache.Statistics.Misses, measurements.Longs["redisnearcache.misses"]);
        Assert.Equal(cache.Statistics.Invalidations, measurements.Longs["redisnearcache.invalidations"]);
        // The sum over the reason tag, which must still be exactly the total these two reported before the breakdown.
        Assert.Equal(cache.Statistics.Flushes, measurements.Longs["redisnearcache.flushes"]);
        Assert.Equal(cache.Statistics.Rearms, measurements.Longs["redisnearcache.rearms"]);
        Assert.Equal(cache.Statistics.RaceDiscards, measurements.Longs["redisnearcache.race_discards"]);
        Assert.Equal(cache.Statistics.L1Entries, measurements.Longs["redisnearcache.l1.entries"]);
        Assert.Equal(cache.Statistics.SerializerFailures, measurements.Longs["redisnearcache.serializer_failures"]);
        Assert.Equal(cache.Statistics.L1StoreRefusals, measurements.Longs["redisnearcache.l1.store_refusals"]);
        Assert.Equal(cache.Statistics.PreArmFailures, measurements.Longs["redisnearcache.prearm_failures"]);
        Assert.Equal(cache.Statistics.LostEndpointCount, measurements.Longs["redisnearcache.endpoints.lost"]);
        Assert.Equal(cache.Statistics.PassThroughSeconds, measurements.Doubles["redisnearcache.pass_through.seconds"]);
        Assert.Equal(1, measurements.Longs["redisnearcache.hits"]);
        Assert.Equal(1, measurements.Longs["redisnearcache.flushes"]);
        Assert.Equal(1, measurements.Longs["redisnearcache.l1.entries"]);
        Assert.Equal(1, measurements.Longs["redisnearcache.coherent"]);
        Assert.Equal(0, measurements.Longs["redisnearcache.endpoints.lost"]);
        Assert.Equal(0, measurements.Longs["redisnearcache.ttl_cap.abandoned"]);
        Assert.Equal(0, measurements.Longs["redisnearcache.untracked_reads.unavailable"]);
        // Coherent, so the pass-through clock is not running.
        Assert.Equal(0d, measurements.Doubles["redisnearcache.pass_through.seconds"]);
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
        Assert.NotEmpty(Collect(clientName).Names);

        await cache.DisposeAsync();

        // The meter is disposed with the cache, so nothing is published for it any more - and asking for a round of
        // measurements while it goes away must not surface an ObjectDisposedException from the L1 store.
        var after = Collect(clientName);
        Assert.Empty(after.Names);
    }

    [Fact]
    public async Task CoherentIsOneWhileArmedAndUnpublishedOnceTheCacheIsGone()
    {
        var clientName = "rnc-metrics-coherent-" + Guid.NewGuid().ToString("N");
        var cache = await StartAsync(clientName);
        Assert.Equal(1, Collect(clientName).Longs["redisnearcache.coherent"]);
        Assert.True(cache.IsCoherent);

        await cache.DisposeAsync();

        Assert.False(cache.IsCoherent);
        // The meter went with the cache, so the gauge is no longer published at all rather than stuck at 1.
        Assert.False(Collect(clientName).Longs.ContainsKey("redisnearcache.coherent"));
    }
}
