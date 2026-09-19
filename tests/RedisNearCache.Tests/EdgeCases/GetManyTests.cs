using RedisNearCache.Tests.Chaos;
using StackExchange.Redis;
using Xunit;
using Xunit.Abstractions;

namespace RedisNearCache.Tests.EdgeCases;

/// <summary>
/// <see cref="IRedisNearCache.GetManyAsync{T}"/> and <see cref="IRedisNearCache.GetManyBytesAsync"/> against a real
/// standalone Redis. The multi-key read is the single-key read fanned out, so what is tested here is that every
/// per-key guarantee still holds when the keys are read together: hits stay hits, misses are tracked and cached,
/// a missing key is present with null and is not cached, a foreign write evicts only its own key, the server-side
/// TTL still caps the entry, keys outside <see cref="RedisNearCacheOptions.KeyPrefixes"/> are still read untracked,
/// and the argument rules are enforced before anything reaches Redis.
/// </summary>
/// <remarks>
/// Every test owns its cache (<c>EdgeCaseSupport.BuildAsync</c>) because they assert on exact
/// <see cref="RedisNearCacheStatistics"/> deltas, and deletes its keys in a finally: the containers are shared
/// with every other test class.
/// </remarks>
public class GetManyTests
{
    private readonly ITestOutputHelper _out;

    public GetManyTests(ITestOutputHelper output) => _out = output;

    /// <summary>
    /// A mix of a key already in L1, a key that exists but has never been read here, and a key that does not
    /// exist: the right value for each, the missing one present with null, and the counters moving by exactly one
    /// per key. Then the same call again: the two existing keys are hits, the missing one misses again, because a
    /// null reply is never cached (<see cref="MissingKeyTests"/>).
    /// </summary>
    [Fact]
    public async Task MixOfCachedUncachedAndMissingKeys()
    {
        var cached = TestHelpers.Key("gm-mix-cached");
        var uncached = TestHelpers.Key("gm-mix-uncached");
        var missing = TestHelpers.Key("gm-mix-missing");
        var handle = await EdgeCaseSupport.BuildAsync(StandaloneCacheFixture.ConnectionString);
        try
        {
            var cache = handle.Cache;
            RedisCli.Standalone("MSET", cached, "c1", uncached, "u1");

            Assert.True(await TestHelpers.ReadUntilCachedAsync(cache, cached, "c1"), $"{cached} was not cached before the multi-key read.");
            Assert.False(cache.TryGetLocal<string>(uncached, out _), $"{uncached} must not be in L1 yet, or the miss path is untested.");
            Assert.False(cache.TryGetLocal<string>(missing, out _), $"{missing} must not be in L1 yet.");

            var hits = cache.Statistics.Hits;
            var misses = cache.Statistics.Misses;

            var first = await cache.GetManyAsync<string>([cached, uncached, missing]);

            Assert.Equal(3, first.Count);
            Assert.Equal("c1", first[cached]);
            Assert.Equal("u1", first[uncached]);
            Assert.True(first.ContainsKey(missing), "a key that does not exist must still be present in the result.");
            Assert.Null(first[missing]);
            Assert.Equal(hits + 1, cache.Statistics.Hits);
            Assert.Equal(misses + 2, cache.Statistics.Misses);

            Assert.True(cache.TryGetLocal<string>(uncached, out var stored) && stored == "u1", "the miss must have been stored in L1.");
            Assert.True(cache.TryGetLocal<string>(cached, out _), "the hit must have left its entry in place.");
            Assert.False(cache.TryGetLocal<string>(missing, out _), "a null reply must never populate L1.");

            hits = cache.Statistics.Hits;
            misses = cache.Statistics.Misses;

            var second = await cache.GetManyAsync<string>([cached, uncached, missing]);

            Assert.Equal("c1", second[cached]);
            Assert.Equal("u1", second[uncached]);
            Assert.Null(second[missing]);
            Assert.Equal(hits + 2, cache.Statistics.Hits);
            Assert.Equal(misses + 1, cache.Statistics.Misses);
        }
        finally
        {
            RedisCli.Standalone("DEL", cached, uncached, missing);
            await handle.DisposeAsync();
        }
    }

    /// <summary>
    /// A foreign write to ONE of several keys read together evicts that key and nothing else, and the next
    /// multi-key read is N-1 hits and exactly one miss, answering with the new value for the written key.
    /// </summary>
    [Fact]
    public async Task AForeignWriteEvictsOnlyItsOwnKeyOfTheSet()
    {
        const int count = 4;
        var keys = Enumerable.Range(0, count).Select(i => TestHelpers.Key($"gm-foreign{i}")).ToArray();
        var handle = await EdgeCaseSupport.BuildAsync(StandaloneCacheFixture.ConnectionString);
        try
        {
            var cache = handle.Cache;
            RedisCli.Standalone([.. new[] { "MSET" }, .. keys.SelectMany((k, i) => new[] { k, $"v{i}" })]);

            var populated = await Poll.UntilAsync(async () =>
            {
                await cache.GetManyAsync<string>(keys);
                return keys.All(k => cache.TryGetLocal<string>(k, out _));
            }, TimeSpan.FromSeconds(10));
            Assert.True(populated, "the multi-key read did not leave every key in L1.");

            var written = keys[1];
            var invalidations = cache.Statistics.Invalidations;
            RedisCli.Standalone("SET", written, "rewritten");

            var evicted = await Poll.UntilAsync(() => !cache.TryGetLocal<string>(written, out _), TimeSpan.FromSeconds(5));
            Assert.True(evicted, "the written key was not evicted from L1 after a foreign SET.");
            Assert.True(cache.Statistics.Invalidations > invalidations, "no invalidation was counted for the foreign write.");
            foreach (var other in keys.Where(k => k != written))
            {
                Assert.True(cache.TryGetLocal<string>(other, out _), $"{other} was evicted although nothing wrote to it.");
            }

            var hits = cache.Statistics.Hits;
            var misses = cache.Statistics.Misses;

            var result = await cache.GetManyAsync<string>(keys);

            Assert.Equal("rewritten", result[written]);
            for (var i = 0; i < count; i++)
            {
                if (keys[i] == written) continue;
                Assert.Equal($"v{i}", result[keys[i]]);
            }

            Assert.Equal(hits + count - 1, cache.Statistics.Hits);
            Assert.Equal(misses + 1, cache.Statistics.Misses);
        }
        finally
        {
            RedisCli.Standalone([.. new[] { "DEL" }, .. keys]);
            await handle.DisposeAsync();
        }
    }

    /// <summary>
    /// The server-side TTL cap still applies to a key read as part of a set: it is cached by the multi-key read,
    /// leaves L1 by the time its TTL has elapsed, and once Redis has dropped it the multi-key read answers null
    /// for it while the key beside it is still served from L1.
    /// </summary>
    /// <remarks>
    /// Which of the two mechanisms drops the entry - the <see cref="RedisNearCacheOptions.RespectServerTtl"/> cap
    /// or the invalidation Redis pushes when it deletes the expired key - is not separated here; isolating the cap
    /// needs the active-expiry pressure <see cref="ServerTtlCapTests"/> builds with its 300k-key filler fixture.
    /// What this test pins is that the multi-key path never serves a value past its TTL.
    /// </remarks>
    [Fact]
    public async Task AKeyWithATtlLeavesL1AndThenReadsAsMissing()
    {
        var expiring = TestHelpers.Key("gm-ttl");
        var control = TestHelpers.Key("gm-ttl-control");
        var handle = await EdgeCaseSupport.BuildAsync(
            StandaloneCacheFixture.ConnectionString,
            o => o.L1MaxAge = TimeSpan.FromMinutes(10));
        try
        {
            var cache = handle.Cache;
            // Long enough that a slow runner (each RedisCli call is a docker exec) still reads it well inside its life.
            RedisCli.Standalone("SET", expiring, "temporary", "PX", "4000");
            RedisCli.Standalone("SET", control, "permanent");

            var first = await cache.GetManyAsync<string>([expiring, control]);
            Assert.Equal("temporary", first[expiring]);
            Assert.Equal("permanent", first[control]);
            Assert.True(cache.TryGetLocal<string>(expiring, out _), "the expiring key must be in L1 first, or the eviction below proves nothing.");
            Assert.True(cache.TryGetLocal<string>(control, out _), "the control key must be in L1 first.");

            // No Redis contact while waiting: the entry must go on its own, well before L1MaxAge (10 minutes).
            var left = await Poll.UntilAsync(() => !cache.TryGetLocal<string>(expiring, out _), TimeSpan.FromSeconds(12));
            Assert.True(left, "the L1 entry outlived the key's own TTL.");
            Assert.True(cache.TryGetLocal<string>(control, out _), "only the expiring key should have left L1.");

            var gone = await Poll.UntilAsync(() => RedisCli.Standalone("EXISTS", expiring) == "0", TimeSpan.FromSeconds(10));
            Assert.True(gone, "Redis still holds the expired key.");

            var hits = cache.Statistics.Hits;
            var second = await cache.GetManyAsync<string>([expiring, control]);

            Assert.Null(second[expiring]);
            Assert.Equal("permanent", second[control]);
            Assert.Equal(hits + 1, cache.Statistics.Hits); // the control key only
            Assert.False(cache.TryGetLocal<string>(expiring, out _), "an expired key must not be served from L1.");
        }
        finally
        {
            RedisCli.Standalone("DEL", expiring, control);
            await handle.DisposeAsync();
        }
    }

    /// <summary>
    /// With <see cref="RedisNearCacheOptions.KeyPrefixes"/> set, a multi-key read mixing prefixed and unprefixed
    /// keys answers correctly for all of them, caches only the prefixed ones, and reads the rest through the
    /// untracked MULTI / CLIENT CACHING NO / GET / EXEC path - counted on the server exactly as
    /// <see cref="OptOutUntrackedReadsTests"/> counts it.
    /// </summary>
    [Fact]
    public async Task OnlyKeysMatchingKeyPrefixesAreCachedAndTheRestAreReadUntracked()
    {
        var p1 = "p:" + TestHelpers.Key("gm-p1");
        var p2 = "p:" + TestHelpers.Key("gm-p2");
        var q1 = "q:" + TestHelpers.Key("gm-q1");
        var q2 = "q:" + TestHelpers.Key("gm-q2");
        var handle = await EdgeCaseSupport.BuildAsync(StandaloneCacheFixture.ConnectionString, o => o.KeyPrefixes.Add("p:"));
        try
        {
            var cache = handle.Cache;
            var server = handle.Multiplexer.GetServer(handle.Multiplexer.GetEndPoints()[0]);
            RedisCli.Standalone("MSET", p1, "pv1", p2, "pv2", q1, "qv1", q2, "qv2");

            var multiBefore = TestHelpers.CommandCalls(server, "multi");
            var execBefore = TestHelpers.CommandCalls(server, "exec");
            var misses = cache.Statistics.Misses;

            var first = await cache.GetManyAsync<string>([p1, q1, p2, q2]);

            Assert.Equal(4, first.Count);
            Assert.Equal("pv1", first[p1]);
            Assert.Equal("pv2", first[p2]);
            Assert.Equal("qv1", first[q1]);
            Assert.Equal("qv2", first[q2]);
            Assert.Equal(misses + 4, cache.Statistics.Misses);

            Assert.True(cache.TryGetLocal<string>(p1, out _), $"{p1} matches KeyPrefixes and should have been cached.");
            Assert.True(cache.TryGetLocal<string>(p2, out _), $"{p2} matches KeyPrefixes and should have been cached.");
            Assert.False(cache.TryGetLocal<string>(q1, out _), "a key outside KeyPrefixes must never be cached in L1.");
            Assert.False(cache.TryGetLocal<string>(q2, out _), "a key outside KeyPrefixes must never be cached in L1.");

            // One transaction per key outside the prefixes, none for the prefixed ones (which were plain GETs).
            Assert.Equal(multiBefore + 2, TestHelpers.CommandCalls(server, "multi"));
            Assert.Equal(execBefore + 2, TestHelpers.CommandCalls(server, "exec"));

            var hits = cache.Statistics.Hits;
            misses = cache.Statistics.Misses;

            var second = await cache.GetManyAsync<string>([p1, q1, p2, q2]);

            Assert.Equal("qv1", second[q1]);
            Assert.Equal("pv1", second[p1]);
            Assert.Equal(hits + 2, cache.Statistics.Hits);   // the two p: keys
            Assert.Equal(misses + 2, cache.Statistics.Misses); // the two q: keys, again
        }
        finally
        {
            RedisCli.Standalone("DEL", p1, p2, q1, q2);
            await handle.DisposeAsync();
        }
    }

    /// <summary>
    /// 2,000 keys in one call: eight windows (seven full ones and a remainder of 208), every value correct, and a second call served
    /// entirely from L1.
    /// </summary>
    [Fact]
    public async Task TwoThousandKeysInOneCallAndThenTwoThousandHits()
    {
        const int count = 2_000;
        const int batchSize = 500;
        var prefix = TestHelpers.Key("gm-large");
        var keys = Enumerable.Range(0, count).Select(i => $"{prefix}:{i}").ToArray();
        var handle = await EdgeCaseSupport.BuildAsync(
            StandaloneCacheFixture.ConnectionString,
            o => o.L1SizeLimit = 20_000);
        await using var foreign = await ForeignClient.ConnectAsync(StandaloneCacheFixture.ConnectionString);
        try
        {
            var cache = handle.Cache;
            for (var start = 0; start < count; start += batchSize)
            {
                var pairs = new KeyValuePair<RedisKey, RedisValue>[Math.Min(batchSize, count - start)];
                for (var i = 0; i < pairs.Length; i++) pairs[i] = new(keys[start + i], $"v-{start + i}");
                await foreign.Db.StringSetAsync(pairs);
            }

            var misses = cache.Statistics.Misses;
            var first = await cache.GetManyAsync<string>(keys);

            Assert.Equal(count, first.Count);
            for (var i = 0; i < count; i++) Assert.Equal($"v-{i}", first[keys[i]]);
            Assert.Equal(misses + count, cache.Statistics.Misses);

            var uncached = keys.Where(k => !cache.TryGetLocal<string>(k, out _)).ToArray();
            Assert.True(uncached.Length == 0, $"{uncached.Length} of {count} keys were not cached, e.g. {string.Join(", ", uncached.Take(3))}");

            var hits = cache.Statistics.Hits;
            misses = cache.Statistics.Misses;

            var second = await cache.GetManyAsync<string>(keys);

            Assert.Equal(count, second.Count);
            for (var i = 0; i < count; i++) Assert.Equal($"v-{i}", second[keys[i]]);
            Assert.Equal(hits + count, cache.Statistics.Hits);
            Assert.Equal(misses, cache.Statistics.Misses);
            _out.WriteLine($"stats after two calls over {count} keys: {cache.Statistics}");
        }
        finally
        {
            for (var start = 0; start < count; start += batchSize)
            {
                var batch = new RedisKey[Math.Min(batchSize, count - start)];
                for (var i = 0; i < batch.Length; i++) batch[i] = keys[start + i];
                await foreign.Db.KeyDeleteAsync(batch, CommandFlags.FireAndForget);
            }

            await handle.DisposeAsync();
        }
    }

    /// <summary>
    /// The raw-bytes form: values that are not valid UTF-8 text, a zero-length value and a missing key, and every
    /// array handed back is the caller's own.
    /// </summary>
    [Fact]
    public async Task GetManyBytesReturnsRawValuesAndCallerOwnedArrays()
    {
        var binary = TestHelpers.Key("gm-bytes-binary");
        var empty = TestHelpers.Key("gm-bytes-empty");
        var missing = TestHelpers.Key("gm-bytes-missing");
        byte[] payload = [0, 1, 2, 250, 255];
        var handle = await EdgeCaseSupport.BuildAsync(StandaloneCacheFixture.ConnectionString);
        try
        {
            var cache = handle.Cache;
            await cache.SetBytesAsync(binary, payload);
            await cache.SetBytesAsync(empty, ReadOnlyMemory<byte>.Empty);

            var first = await cache.GetManyBytesAsync([binary, empty, missing]);

            Assert.Equal(3, first.Count);
            Assert.Equal(payload, first[binary]);
            Assert.Equal(Array.Empty<byte>(), first[empty]);
            Assert.True(first.ContainsKey(missing), "a missing key must still be present in the bytes result.");
            Assert.Null(first[missing]);

            Assert.True(await Poll.UntilAsync(async () =>
            {
                await cache.GetManyBytesAsync([binary]);
                return cache.TryGetLocal<byte[]>(binary, out _);
            }, TimeSpan.FromSeconds(5)), "the binary key was never cached, so the copy rule below would be untested.");

            // Mutating what the call handed back must not change what the cache holds.
            var second = await cache.GetManyBytesAsync([binary]);
            second[binary]![0] = 99;
            var third = await cache.GetManyBytesAsync([binary]);

            Assert.Equal(payload, third[binary]);
            Assert.NotSame(second[binary], third[binary]);
        }
        finally
        {
            RedisCli.Standalone("DEL", binary, empty, missing);
            await handle.DisposeAsync();
        }
    }

    [Fact]
    public async Task DuplicateKeysAreReadOnceAgainstARealRedis()
    {
        var key = TestHelpers.Key("gm-duplicate");
        var handle = await EdgeCaseSupport.BuildAsync(StandaloneCacheFixture.ConnectionString);
        try
        {
            var cache = handle.Cache;
            var server = handle.Multiplexer.GetServer(handle.Multiplexer.GetEndPoints()[0]);
            RedisCli.Standalone("SET", key, "v1");

            var getsBefore = TestHelpers.CommandCalls(server, "get");
            var misses = cache.Statistics.Misses;
            var hits = cache.Statistics.Hits;

            var result = await cache.GetManyAsync<string>([key, key, key]);

            Assert.Single(result);
            Assert.Equal("v1", result[key]);
            Assert.Equal(misses + 1, cache.Statistics.Misses);
            Assert.Equal(hits, cache.Statistics.Hits);
            Assert.Equal(getsBefore + 1, TestHelpers.CommandCalls(server, "get"));
        }
        finally
        {
            RedisCli.Standalone("DEL", key);
            await handle.DisposeAsync();
        }
    }

    [Fact]
    public async Task ABadKeyListIsRefusedBeforeAnythingReachesRedis()
    {
        var key = TestHelpers.Key("gm-argument");
        var handle = await EdgeCaseSupport.BuildAsync(StandaloneCacheFixture.ConnectionString);
        try
        {
            var cache = handle.Cache;
            var server = handle.Multiplexer.GetServer(handle.Multiplexer.GetEndPoints()[0]);
            RedisCli.Standalone("SET", key, "v1");

            var getsBefore = TestHelpers.CommandCalls(server, "get");
            var misses = cache.Statistics.Misses;

            await Assert.ThrowsAsync<ArgumentNullException>(() => cache.GetManyAsync<string>(null!).AsTask());
            var ex = await Assert.ThrowsAsync<ArgumentException>(() => cache.GetManyAsync<string>([key, null!]).AsTask());
            var bytesEx = await Assert.ThrowsAsync<ArgumentException>(() => cache.GetManyBytesAsync([key, null!]).AsTask());

            Assert.Equal("keys", ex.ParamName);
            Assert.Equal("keys", bytesEx.ParamName);
            Assert.Equal(misses, cache.Statistics.Misses);
            Assert.Equal(getsBefore, TestHelpers.CommandCalls(server, "get"));
            Assert.False(cache.TryGetLocal<string>(key, out _), "a refused call must not have read, let alone cached, the valid key beside the null one.");

            // Positive control: the same key in a well-formed call IS read and cached, so the assertions above
            // are about the refusal and not about a key that could never have been cached anyway.
            Assert.Equal("v1", (await cache.GetManyAsync<string>([key]))[key]);
            Assert.Equal(misses + 1, cache.Statistics.Misses);
            Assert.Equal(getsBefore + 1, TestHelpers.CommandCalls(server, "get"));
            Assert.True(cache.TryGetLocal<string>(key, out _), "the well-formed call should have cached the key.");
        }
        finally
        {
            RedisCli.Standalone("DEL", key);
            await handle.DisposeAsync();
        }
    }

    [Fact]
    public async Task APreCancelledTokenIsRefusedEvenWhenEveryKeyIsInL1()
    {
        var a = TestHelpers.Key("gm-cancel-a");
        var b = TestHelpers.Key("gm-cancel-b");
        var handle = await EdgeCaseSupport.BuildAsync(StandaloneCacheFixture.ConnectionString);
        try
        {
            var cache = handle.Cache;
            RedisCli.Standalone("MSET", a, "av", b, "bv");
            Assert.True(await TestHelpers.ReadUntilCachedAsync(cache, a, "av"), $"{a} was not cached.");
            Assert.True(await TestHelpers.ReadUntilCachedAsync(cache, b, "bv"), $"{b} was not cached.");

            var hits = cache.Statistics.Hits;

            await Assert.ThrowsAnyAsync<OperationCanceledException>(
                () => cache.GetManyAsync<string>([a, b], new CancellationToken(true)).AsTask());
            await Assert.ThrowsAnyAsync<OperationCanceledException>(
                () => cache.GetManyBytesAsync([a, b], new CancellationToken(true)).AsTask());

            Assert.Equal(hits, cache.Statistics.Hits);
            Assert.True(cache.TryGetLocal<string>(a, out _), "a refused read must leave the entries it did not serve alone.");
            Assert.True(cache.TryGetLocal<string>(b, out _), "a refused read must leave the entries it did not serve alone.");
        }
        finally
        {
            RedisCli.Standalone("DEL", a, b);
            await handle.DisposeAsync();
        }
    }
}
