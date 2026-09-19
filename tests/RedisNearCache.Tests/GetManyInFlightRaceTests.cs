using Microsoft.Extensions.DependencyInjection;
using Xunit;
using Xunit.Abstractions;

namespace RedisNearCache.Tests;

/// <summary>
/// The no-stale-read rule under <see cref="IRedisNearCache.GetManyAsync{T}"/>: a reply overtaken by an
/// invalidation while it was in flight must not populate L1, and that must stay true for one key of a set while
/// every other key of the same call is stored as usual.
/// </summary>
/// <remarks>
/// Built exactly like <see cref="InFlightRaceDiscardsReplyTests"/>: its own provider, so the internal
/// <c>TestHooks.AfterRedisReadBeforeStore</c> captured from the configure callback can inject a foreign write
/// between the Redis reply and the L1 store. The hook runs once per key, so it acts only for the chosen key and
/// returns immediately for the others.
/// </remarks>
public class GetManyInFlightRaceTests : IAsyncLifetime
{
    private readonly ITestOutputHelper _out;
    private ServiceProvider _provider = null!;
    private IRedisNearCache _cache = null!;
    private RedisNearCacheOptions _options = null!;

    public GetManyInFlightRaceTests(ITestOutputHelper output) => _out = output;

    public async Task InitializeAsync()
    {
        var services = new ServiceCollection();
        services.AddRedisNearCache(StandaloneCacheFixture.ConnectionString, o => _options = o);
        _provider = services.BuildServiceProvider();
        _cache = _provider.GetRequiredService<IRedisNearCache>();
        await _cache.Ready;
    }

    public async Task DisposeAsync() => await _provider.DisposeAsync();

    /// <summary>
    /// A whole-cache flush (what a reconnect, a re-arm or a FLUSHDB does) landing while a multi-key read is in flight:
    /// every read of the call began before the flush, so none of their replies may be stored, whichever side of the
    /// flush each reply arrived on. Every key of one window is started before any reply is handled, which is what
    /// makes "all of them" the exact expectation here.
    /// </summary>
    [Fact]
    public async Task AFlushInTheMiddleOfAMultiKeyReadLeavesNoneOfItsKeysInL1()
    {
        const int count = 6;
        var keys = Enumerable.Range(0, count).Select(i => TestHelpers.Key($"gm-flush{i}")).ToArray();
        try
        {
            RedisCli.Standalone([.. new[] { "MSET" }, .. keys.SelectMany(k => new[] { k, "v1" })]);

            var flushed = 0;
            _options.TestHooks.AfterRedisReadBeforeStore = _ =>
            {
                // Once, from whichever reply is handled first: by then every read of the window is in flight.
                if (Interlocked.Exchange(ref flushed, 1) == 0) _cache.EvictAllLocal();
                return Task.CompletedTask;
            };

            var flushes = _cache.Statistics.Flushes;
            var result = await _cache.GetManyAsync<string>(keys);
            _options.TestHooks.AfterRedisReadBeforeStore = null;

            Assert.Equal(1, flushed);
            Assert.Equal(flushes + 1, _cache.Statistics.Flushes);
            foreach (var key in keys) Assert.Equal("v1", result[key]);
            foreach (var key in keys)
            {
                Assert.False(_cache.TryGetLocal<string>(key, out _),
                    $"{key} was stored although its read began before a whole-cache flush.");
            }

            // Not a permanent state: the same keys cache normally on the next call.
            await _cache.GetManyAsync<string>(keys);
            foreach (var key in keys)
            {
                Assert.True(_cache.TryGetLocal<string>(key, out var cached) && cached == "v1", $"{key} did not cache after the flush.");
            }
        }
        finally
        {
            _options.TestHooks.AfterRedisReadBeforeStore = null;
            RedisCli.Standalone([.. new[] { "DEL" }, .. keys]);
        }
    }

    [Fact]
    public async Task AKeyInvalidatedWhileItsReplyWasInFlightIsDiscardedAndTheRestOfTheSetIsCached()
    {
        const int count = 5;
        var keys = Enumerable.Range(0, count).Select(i => TestHelpers.Key($"gm-race{i}")).ToArray();
        var raced = keys[2];
        try
        {
            RedisCli.Standalone([.. new[] { "MSET" }, .. keys.SelectMany(k => new[] { k, "v1" })]);

            var hookRuns = 0;
            var hookFired = 0;
            _options.TestHooks.AfterRedisReadBeforeStore = async key =>
            {
                Interlocked.Increment(ref hookRuns);
                if (!string.Equals(key, raced, StringComparison.Ordinal)) return;

                // The GET for this key has already been answered ("v1") and the server is therefore tracking it.
                // Write it from outside and wait until the invalidation has actually been delivered, so the store
                // below is guaranteed to be racing a known-superseded value rather than merely maybe racing one.
                Interlocked.Increment(ref hookFired);
                var invalidations = _cache.Statistics.Invalidations;
                RedisCli.Standalone("SET", key, "v2");
                await Poll.UntilAsync(() => _cache.Statistics.Invalidations > invalidations, TimeSpan.FromSeconds(10));
            };

            var discards = _cache.Statistics.RaceDiscards;
            var result = await _cache.GetManyAsync<string>(keys);
            _options.TestHooks.AfterRedisReadBeforeStore = null;

            Assert.Equal(count, hookRuns);
            Assert.Equal(1, hookFired);

            // Redis answered "v1" for every key, including the raced one: the call returns what the server said.
            foreach (var key in keys) Assert.Equal("v1", result[key]);

            Assert.False(_cache.TryGetLocal<string>(raced, out var stale),
                $"a reply for a key invalidated in flight must not populate L1, but L1 holds '{stale}'.");
            Assert.Equal(discards + 1, _cache.Statistics.RaceDiscards);

            foreach (var other in keys.Where(k => k != raced))
            {
                Assert.True(_cache.TryGetLocal<string>(other, out var cached) && cached == "v1",
                    $"{other} was not stored although nothing invalidated it; the discard must be per key.");
            }

            _out.WriteLine($"stats after the raced multi-key read: {_cache.Statistics}");

            // And the next read of the raced key sees what Redis actually holds now.
            Assert.Equal("v2", (await _cache.GetManyAsync<string>([raced]))[raced]);
        }
        finally
        {
            _options.TestHooks.AfterRedisReadBeforeStore = null;
            RedisCli.Standalone([.. new[] { "DEL" }, .. keys]);
        }
    }

    /// <summary>
    /// The same race against <see cref="IRedisNearCache.GetManyBytesAsync"/>, which reaches L1 through the same
    /// stored-bytes path.
    /// </summary>
    [Fact]
    public async Task TheSameRaceIsDiscardedForTheBytesForm()
    {
        var raced = TestHelpers.Key("gm-race-bytes");
        var other = TestHelpers.Key("gm-race-bytes-other");
        try
        {
            RedisCli.Standalone("MSET", raced, "v1", other, "v1");

            _options.TestHooks.AfterRedisReadBeforeStore = async key =>
            {
                if (!string.Equals(key, raced, StringComparison.Ordinal)) return;
                var invalidations = _cache.Statistics.Invalidations;
                RedisCli.Standalone("SET", key, "v2");
                await Poll.UntilAsync(() => _cache.Statistics.Invalidations > invalidations, TimeSpan.FromSeconds(10));
            };

            var discards = _cache.Statistics.RaceDiscards;
            var result = await _cache.GetManyBytesAsync([raced, other]);
            _options.TestHooks.AfterRedisReadBeforeStore = null;

            Assert.Equal("v1"u8.ToArray(), result[raced]);
            Assert.Equal("v1"u8.ToArray(), result[other]);
            Assert.False(_cache.TryGetLocal<byte[]>(raced, out _), "a bytes reply invalidated in flight must not populate L1 either.");
            Assert.Equal(discards + 1, _cache.Statistics.RaceDiscards);
            Assert.True(_cache.TryGetLocal<byte[]>(other, out _), "the key nothing wrote to must still have been stored.");

            Assert.Equal("v2"u8.ToArray(), (await _cache.GetManyBytesAsync([raced]))[raced]);
        }
        finally
        {
            _options.TestHooks.AfterRedisReadBeforeStore = null;
            RedisCli.Standalone("DEL", raced, other);
        }
    }
}
