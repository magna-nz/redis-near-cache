using RedisNearCache.Tests.Chaos;
using StackExchange.Redis;
using Xunit;
using Xunit.Abstractions;

namespace RedisNearCache.Tests.Compat;

/// <summary>
/// A key can leave Redis without anybody writing to it: under <c>maxmemory</c> with an <c>allkeys-*</c>
/// policy the server evicts keys on its own. Redis treats that as a modification and pushes an invalidation,
/// so the near cache must drop its copy too - otherwise L1 would keep serving a value that no longer exists
/// anywhere on the server, and no future write would ever correct it (the key is not tracked any more).
/// </summary>
// maxmemory is a server-wide setting and the filler writes evict other keys in the same database, so this
// class shares the serialized "flush" collection with the other whole-server tests.
[Collection("flush")]
public class EvictionUnderMaxMemoryInvalidatesTests : IClassFixture<StandaloneCacheFixture>
{
    private const int FillerValueBytes = 64 * 1024;
    private const int FillerKeyCount = 160;   // ~10 MB of filler against a 2 MB limit.

    private readonly StandaloneCacheFixture _fx;
    private readonly ITestOutputHelper _out;

    public EvictionUnderMaxMemoryInvalidatesTests(StandaloneCacheFixture fx, ITestOutputHelper output)
    {
        _fx = fx;
        _out = output;
    }

    [Fact]
    public async Task ServerSideEvictionEvictsL1()
    {
        var originalMaxMemory = CompatSupport.ConfigGet("maxmemory");
        var originalPolicy = CompatSupport.ConfigGet("maxmemory-policy");
        _out.WriteLine($"original maxmemory='{originalMaxMemory}' maxmemory-policy='{originalPolicy}'");

        var key = TestHelpers.Key("maxmem-tracked");
        var fillerPrefix = TestHelpers.Key("maxmem-filler");
        await using var foreign = await ForeignClient.ConnectAsync(StandaloneCacheFixture.ConnectionString);

        try
        {
            await _fx.Cache.SetAsync(key, "tracked-value");
            Assert.True(
                await TestHelpers.ReadUntilCachedAsync(_fx.Cache, key, "tracked-value"),
                "the tracked key was not cached locally before memory pressure was applied.");

            CompatSupport.ConfigSet("maxmemory-policy", "allkeys-lru");

            // A flat 2 MB limit is below what an otherwise idle Redis reports as used_memory on several of the
            // matrix's server builds (7.4 reports ~2.8 MB with an empty dataset). Setting it would evict the
            // tracked key instantly, before a single filler byte was written, and the test would pass without
            // ever exercising memory pressure. So the limit is 2 MB of HEADROOM above the current usage: the
            // key must survive the CONFIG SET and die from the filler.
            var used = UsedMemoryBytes();
            var limit = Math.Max(2L * 1024 * 1024, used + (2L * 1024 * 1024));
            CompatSupport.ConfigSet("maxmemory", limit.ToString());
            _out.WriteLine($"used_memory={used} bytes; maxmemory set to {limit} bytes");

            Assert.Equal("1", RedisCli.Standalone("EXISTS", key));

            var filler = new string('x', FillerValueBytes);
            var written = 0;
            var oom = false;
            for (var i = 0; i < FillerKeyCount; i++)
            {
                try
                {
                    await foreign.Db.StringSetAsync($"{fillerPrefix}:{i}", filler);
                    written++;
                }
                catch (RedisServerException ex) when (ex.Message.Contains("OOM", StringComparison.Ordinal))
                {
                    // The server hit the limit and had nothing left it was willing to evict. That is already
                    // more pressure than the test needs; what matters is whether the tracked key survived.
                    _out.WriteLine($"filler write {i} refused with OOM; stopping after {written} writes.");
                    oom = true;
                    break;
                }

                // Each of these is a docker exec (~80 ms), so the tracked key is only checked every 10 writes.
                if (i % 10 == 9 && RedisCli.Standalone("EXISTS", key) == "0") break;
            }

            _out.WriteLine($"wrote {written} filler keys of {FillerValueBytes} bytes ({written * (long)FillerValueBytes / (1024 * 1024)} MB); oom={oom}");

            var evictedOnServer = await Poll.UntilAsync(
                () => RedisCli.Standalone("EXISTS", key) == "0",
                TimeSpan.FromSeconds(5),
                TimeSpan.FromMilliseconds(100));
            Assert.True(
                evictedOnServer,
                $"the server never evicted the tracked key after {written} filler writes of {FillerValueBytes} bytes under a {limit} byte maxmemory.");

            var evictedLocally = await Poll.UntilAsync(
                () => !_fx.Cache.TryGetLocal<string>(key, out _),
                TimeSpan.FromSeconds(5));
            Assert.True(
                evictedLocally,
                "the server evicted the key under memory pressure but no invalidation reached L1 within 5 s.");

            Assert.Null(await _fx.Cache.GetAsync<string>(key));
        }
        finally
        {
            // Restore the limit BEFORE deleting the filler: a DEL under a breached maxmemory is still allowed,
            // but the next test in this collection must not run under an eviction policy it did not ask for.
            CompatSupport.ConfigSet("maxmemory", string.IsNullOrEmpty(originalMaxMemory) ? "0" : originalMaxMemory);
            CompatSupport.ConfigSet("maxmemory-policy", string.IsNullOrEmpty(originalPolicy) ? "noeviction" : originalPolicy);

            var keys = Enumerable.Range(0, FillerKeyCount)
                .Select(i => (RedisKey)$"{fillerPrefix}:{i}")
                .ToArray();
            await foreign.Db.KeyDeleteAsync(keys);
            await foreign.Db.KeyDeleteAsync(key);
        }

        // Outside the finally on purpose: a failed assertion here would otherwise replace the real failure.
        Assert.Equal(originalMaxMemory, CompatSupport.ConfigGet("maxmemory"));
        Assert.Equal(originalPolicy, CompatSupport.ConfigGet("maxmemory-policy"));
    }

    private static long UsedMemoryBytes() =>
        long.TryParse(CompatSupport.Info("memory").GetValueOrDefault("used_memory"), out var used) ? used : 0;
}
