using System.Diagnostics;
using StackExchange.Redis;
using Xunit;
using Xunit.Abstractions;

namespace RedisNearCache.Tests.EdgeCases;

/// <summary>
/// <see cref="RedisNearCacheOptions.RespectServerTtl"/>: Redis only pushes an expiry invalidation when its
/// active-expiry cycle actually deletes a key, and that cycle samples roughly 20-25 keys per database per
/// 100 ms. On a database with a large number of TTL-carrying keys, an already-expired key can sit undeleted
/// (and therefore un-invalidated) for a long time, and nothing else touches it locally because hits are
/// served from L1. <c>DEBUG SET-ACTIVE-EXPIRE</c> is disabled on these containers, so these tests recreate
/// that pressure directly: <see cref="TtlFillerFixture"/> fills db 0 with 300k long-TTL keys once for the class,
/// so active expiry (a cursor sweep of ~25 keys per 100 ms) cannot plausibly reach the key under test inside a
/// test's own short window: a few hundred milliseconds of exposure against 300k keys is well under a 0.1% chance.
/// </summary>
public class ServerTtlCapTests : IClassFixture<TtlFillerFixture>
{
    private readonly ITestOutputHelper _out;

    public ServerTtlCapTests(TtlFillerFixture filler, ITestOutputHelper output)
    {
        _out = output;
        Assert.True(filler.Filled, "the TTL filler keys were not written; active-expiry pressure would be understated.");
    }

    [Fact]
    public async Task EntryLeavesL1WhenItsTtlElapsesEvenThoughRedisHasNotExpiredItYet()
    {
        var key = TestHelpers.Key("ttlcap-on");
        var handle = await EdgeCaseSupport.BuildAsync(StandaloneCacheFixture.ConnectionString, o => o.L1MaxAge = TimeSpan.FromMinutes(10));
        try
        {
            // 3 s: long enough that the first read is never racing the expiry, short enough to observe.
            await handle.Cache.SetAsync(key, "v1", expiry: TimeSpan.FromMilliseconds(3000));
            var sw = Stopwatch.StartNew();
            Assert.True(await TestHelpers.ReadUntilCachedAsync(handle.Cache, key, "v1"), "the key was not cached right after being read.");

            var invalidationsBefore = handle.Cache.Statistics.Invalidations;
            var evicted = await Poll.UntilAsync(
                () => !handle.Cache.TryGetLocal<string>(key, out _),
                TimeSpan.FromMilliseconds(4500), TimeSpan.FromMilliseconds(20));
            sw.Stop();
            _out.WriteLine($"L1 entry left the cache after {sw.ElapsedMilliseconds} ms (key TTL was 3000 ms, L1MaxAge is 10 minutes).");
            Assert.True(evicted,
                "RespectServerTtl should cap the L1 entry at the key's own remaining TTL and evict it once that TTL elapses, " +
                "well before L1MaxAge (10 minutes) would.");

            // The key expired ~20 ms ago at most (the poll step), so the only way it could already have been deleted and
            // invalidated is active expiry sampling it in that window: ~5 keys out of 300k.
            Assert.True(handle.Cache.Statistics.Invalidations == invalidationsBefore,
                "no expiry invalidation should have arrived here: active expiry cannot plausibly have reached this one key " +
                "among the filler keys in this short a window. If this ever fails, the filler was too small or the server " +
                "too fast, not the library.");

            Assert.Equal("-2", RedisCli.Standalone("TTL", key));

            Assert.Null(await handle.Cache.GetAsync<string>(key));
        }
        finally
        {
            RedisCli.Standalone("DEL", key);
            await handle.DisposeAsync();
        }
    }

    [Fact]
    public async Task WithRespectServerTtlOffTheEntryOutlivesItsTtlUntilAnInvalidationArrives()
    {
        var key = TestHelpers.Key("ttlcap-off");
        var handle = await EdgeCaseSupport.BuildAsync(StandaloneCacheFixture.ConnectionString, o =>
        {
            o.L1MaxAge = TimeSpan.FromMinutes(10);
            o.RespectServerTtl = false;
        });
        try
        {
            await handle.Cache.SetAsync(key, "v1", expiry: TimeSpan.FromMilliseconds(2000));
            var sw = Stopwatch.StartNew();
            Assert.True(await TestHelpers.ReadUntilCachedAsync(handle.Cache, key, "v1"), "the key was not cached right after being read.");

            // Deliberately a fixed wait: the claim is that nothing evicts the entry during the ~400 ms after its TTL.
            var remaining = TimeSpan.FromMilliseconds(2400) - sw.Elapsed;
            if (remaining > TimeSpan.Zero) await Task.Delay(remaining);
            Assert.True(handle.Cache.TryGetLocal<string>(key, out var stale) && stale == "v1",
                "with RespectServerTtl=false the old behaviour serves the value past its own TTL until an invalidation " +
                "arrives; this is the documented gap the option guards against.");

            Assert.Equal("0", RedisCli.Standalone("EXISTS", key));

            var evicted = await Poll.UntilAsync(() => !handle.Cache.TryGetLocal<string>(key, out _), TimeSpan.FromSeconds(3));
            Assert.True(evicted, "the lazy expiry triggered by EXISTS should have pushed an invalidation that evicted the L1 entry.");
        }
        finally
        {
            RedisCli.Standalone("DEL", key);
            await handle.DisposeAsync();
        }
    }

    [Fact]
    public async Task MissSendsPttlAlongsideGet()
    {
        var handle = await EdgeCaseSupport.BuildAsync(StandaloneCacheFixture.ConnectionString);
        var key = TestHelpers.Key("pttl-pipelined");
        try
        {
            var server = handle.Multiplexer.GetServer(handle.Multiplexer.GetEndPoints()[0]);
            await handle.Cache.SetAsync(key, "v1");

            var pttlBefore = TestHelpers.CommandCalls(server, "pttl");
            var getBefore = TestHelpers.CommandCalls(server, "get");

            Assert.Equal("v1", await handle.Cache.GetAsync<string>(key));
            Assert.Equal(pttlBefore + 1, TestHelpers.CommandCalls(server, "pttl"));
            Assert.Equal(getBefore + 1, TestHelpers.CommandCalls(server, "get"));

            // Second read is a hit: no further GET or PTTL.
            Assert.Equal("v1", await handle.Cache.GetAsync<string>(key));
            Assert.Equal(pttlBefore + 1, TestHelpers.CommandCalls(server, "pttl"));
            Assert.Equal(getBefore + 1, TestHelpers.CommandCalls(server, "get"));

            Assert.True(handle.Cache.TryGetLocal<string>(key, out var cached), "a key with no TTL should still be cached, capped only by L1MaxAge.");
            Assert.Equal("v1", cached);
        }
        finally
        {
            RedisCli.Standalone("DEL", key);
            await handle.DisposeAsync();
        }
    }

    // Test D (KeyThatVanishesBetweenGetAndPttlIsServedButNotCached) is intentionally omitted: it needs a
    // test hook (to race a delete between GET and PTTL) that does not exist in RedisNearCacheTestHooks.
}

/// <summary>
/// 300k keys with a one-hour TTL in db 0 of the standalone container, written once per test class through a
/// foreign multiplexer and deleted again on dispose. See <see cref="ServerTtlCapTests"/>.
/// </summary>
public sealed class TtlFillerFixture : IAsyncLifetime
{
    public const int FillerCount = 300_000;
    private const int ChunkSize = 10_000;

    private readonly string _fillerPrefix = "t:ttlfill:" + Guid.NewGuid();
    private ConnectionMultiplexer _filler = null!;

    public bool Filled { get; private set; }

    public async Task InitializeAsync()
    {
        _filler = await ConnectionMultiplexer.ConnectAsync(StandaloneCacheFixture.ConnectionString);
        var db = _filler.GetDatabase();
        for (var start = 0; start < FillerCount; start += ChunkSize)
        {
            var batch = db.CreateBatch();
            for (var i = start; i < start + ChunkSize; i++)
            {
                _ = batch.StringSetAsync($"{_fillerPrefix}{i}", "x", TimeSpan.FromHours(1), When.Always, CommandFlags.FireAndForget);
            }
            batch.Execute();
        }
        await db.PingAsync();
        Filled = await db.KeyExistsAsync($"{_fillerPrefix}{FillerCount - 1}");
    }

    public async Task DisposeAsync()
    {
        try
        {
            var db = _filler.GetDatabase();
            for (var start = 0; start < FillerCount; start += ChunkSize)
            {
                var batch = db.CreateBatch();
                for (var i = start; i < start + ChunkSize; i++)
                {
                    _ = batch.KeyDeleteAsync($"{_fillerPrefix}{i}", CommandFlags.FireAndForget);
                }
                batch.Execute();
            }
            await db.PingAsync();
        }
        finally
        {
            _filler.Dispose();
        }
    }
}
