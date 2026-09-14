using System.Diagnostics;
using System.Globalization;
using RedisNearCache.Tests.Chaos;
using RedisNearCache.Tests.EdgeCases;
using StackExchange.Redis;
using Xunit;
using Xunit.Abstractions;

namespace RedisNearCache.Tests.Resilience;

/// <summary>
/// <c>CLIENT PAUSE ... ALL</c> is the one failure mode where the connection stays perfectly healthy and the
/// server simply stops answering: a failover window, a slow <c>BGSAVE</c>, a blocking <c>DEBUG SLEEP</c>. It is
/// interesting for a near cache because nothing fails, so nothing re-arms and nothing flushes - the cache must
/// simply stall and then carry on with L1 still coherent.
/// </summary>
/// <remarks>
/// Two variants, split around StackExchange.Redis's 5 s default sync/async timeout:
/// a 3 s pause, which reads ride out, and an 8 s pause, which they cannot. The pause cannot be cut short:
/// <c>CLIENT UNPAUSE</c> is itself postponed by the pause (measured against the redis:7.4 container: an
/// UNPAUSE issued immediately after <c>CLIENT PAUSE 3000 ALL</c> returned after 3056 ms), so the tests wait the
/// pause out rather than trying to cancel it.
/// </remarks>
public class ClientPauseTests
{
    private readonly ITestOutputHelper _out;

    public ClientPauseTests(ITestOutputHelper output) => _out = output;

    private static void Pause(int milliseconds) =>
        RedisCli.Standalone("CLIENT", "PAUSE", milliseconds.ToString(CultureInfo.InvariantCulture), "ALL");

    /// <summary>
    /// 3 s: under the 5 s timeout. Reads block for most of the pause and then succeed, L1 is untouched
    /// throughout (no reconnect, so no flush and no re-arm), and tracking still works afterwards.
    /// </summary>
    [Fact]
    public async Task ClientPauseShorterThanTimeoutStallsReadsThenRecovers()
    {
        var handle = await EdgeCaseSupport.BuildAsync(StandaloneCacheFixture.ConnectionString);
        var key = TestHelpers.Key("pause-stall");
        try
        {
            var cache = handle.Cache;
            RedisCli.Standalone("SET", key, "v1");
            Assert.True(await TestHelpers.ReadUntilCachedAsync(cache, key, "v1"), "key was not cached before the pause.");

            var flushesBefore = cache.Statistics.Flushes;
            var rearmsBefore = cache.Statistics.Rearms;

            // Force the read to reach Redis: a local hit would not be paused at all.
            cache.EvictLocal(key);

            Pause(3_000);
            var sw = Stopwatch.StartNew();
            var value = await cache.GetAsync<string>(key);
            sw.Stop();

            _out.WriteLine($"read under a 3 s pause returned '{value}' after {sw.ElapsedMilliseconds} ms; {cache.Statistics}");
            Assert.Equal("v1", value);
            Assert.True(sw.ElapsedMilliseconds >= 1_000,
                $"the read was not stalled by CLIENT PAUSE at all (took {sw.ElapsedMilliseconds} ms); the pause did not take effect.");

            // Nothing failed, so nothing may have flushed or re-armed.
            Assert.Equal(flushesBefore, cache.Statistics.Flushes);
            Assert.Equal(rearmsBefore, cache.Statistics.Rearms);

            // And the key is coherent again: a foreign write evicts.
            Assert.True(await TestHelpers.ReadUntilCachedAsync(cache, key, "v1"), "key was not cached after the pause.");
            RedisCli.Standalone("SET", key, "v2");
            var evicted = await Poll.UntilAsync(() => !cache.TryGetLocal<string>(key, out _), TimeSpan.FromSeconds(5));
            Assert.True(evicted, "tracking stopped delivering invalidations after a CLIENT PAUSE.");
            Assert.Equal("v2", await cache.GetAsync<string>(key));
        }
        finally
        {
            await handle.DisposeAsync();
            RedisCli.Standalone("DEL", key);
        }
    }

    /// <summary>
    /// 8 s: past the 5 s timeout, so the read fails. The point is what the caller sees and what L1 is left
    /// holding - the exception must surface (not be swallowed into a null, which would look like a cache miss
    /// to the caller) and no entry may be left behind that disagrees with Redis.
    /// </summary>
    [Fact]
    public async Task ClientPauseLongerThanTimeoutSurfacesTimeoutAndRecovers()
    {
        var handle = await EdgeCaseSupport.BuildAsync(StandaloneCacheFixture.ConnectionString);
        ForeignClient? truth = null;
        var key = TestHelpers.Key("pause-timeout");
        try
        {
            var cache = handle.Cache;
            RedisCli.Standalone("SET", key, "v1");
            Assert.True(await TestHelpers.ReadUntilCachedAsync(cache, key, "v1"), "key was not cached before the pause.");
            cache.EvictLocal(key);

            Pause(8_000);
            var sw = Stopwatch.StartNew();
            var failure = await Record.ExceptionAsync(async () => await cache.GetAsync<string>(key));
            sw.Stop();

            _out.WriteLine($"read under an 8 s pause threw {failure?.GetType().Name ?? "<nothing>"} after {sw.ElapsedMilliseconds} ms");
            Assert.NotNull(failure);
            Assert.IsAssignableFrom<RedisTimeoutException>(failure);

            // Wait the pause out. The server is responsive again once a plain PING from redis-cli returns
            // promptly; redis-cli's own command is postponed by the pause, so this is a real condition.
            RedisCli.Standalone("PING");

            // Recovery: reads work, nothing stale was left behind, and tracking still evicts.
            var recovered = await ChaosSupport.WithReconnectRetryAsync(
                async () => await cache.GetAsync<string>(key), TimeSpan.FromSeconds(30));
            Assert.Equal("v1", recovered);

            Assert.True(await ChaosSupport.QuiesceAsync(cache), "the cache never settled after the pause.");
            truth = await ForeignClient.ConnectAsync(StandaloneCacheFixture.ConnectionString);
            var stale = await ChaosSupport.FindStaleAsync(cache, truth, [key]);
            Assert.True(stale.Count == 0, "L1 disagreed with Redis after a timed-out read:\n" + string.Join("\n", stale));

            Assert.True(await TestHelpers.ReadUntilCachedAsync(cache, key, "v1", TimeSpan.FromSeconds(30)),
                "key was not cached again after the pause expired.");
            RedisCli.Standalone("SET", key, "v2");
            var evicted = await Poll.UntilAsync(() => !cache.TryGetLocal<string>(key, out _), TimeSpan.FromSeconds(10));
            Assert.True(evicted, "tracking stopped delivering invalidations after a read timed out under CLIENT PAUSE.");
            Assert.Equal("v2", await cache.GetAsync<string>(key));
            _out.WriteLine($"after recovery: {cache.Statistics}");
        }
        finally
        {
            if (truth is not null) await truth.DisposeAsync();
            await handle.DisposeAsync();
            RedisCli.Standalone("DEL", key);
        }
    }
}
