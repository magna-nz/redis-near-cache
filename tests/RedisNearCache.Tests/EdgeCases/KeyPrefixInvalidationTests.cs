using Xunit;

namespace RedisNearCache.Tests.EdgeCases;

/// <summary>
/// <see cref="RedisNearCacheOptions.KeyPrefixes"/> decides both what gets stored in L1 and what the server tracks:
/// the connection is armed in OPTOUT mode and a read of a key outside every prefix is sent with
/// <c>CLIENT CACHING NO</c> (inside a MULTI/EXEC so the two are adjacent on the wire), so the server never
/// remembers it and a later write to it pushes nothing. A key inside a prefix is tracked and invalidated as
/// before. (Before 0.5.2 every key read through the connection was tracked whatever its prefix.)
/// </summary>
public class KeyPrefixInvalidationTests
{
    [Fact]
    public async Task KeyPrefixesStillInvalidateCachedPrefix()
    {
        var handle = await EdgeCaseSupport.BuildAsync(StandaloneCacheFixture.ConnectionString, o => o.KeyPrefixes.Add("a:"));
        var cachedKey = "a:" + TestHelpers.Key("prefix-in");
        var uncachedKey = "b:" + TestHelpers.Key("prefix-out");
        try
        {
            var cache = handle.Cache;

            await cache.SetAsync(cachedKey, "v1");
            Assert.Equal("v1", await cache.GetAsync<string>(cachedKey));
            Assert.True(cache.TryGetLocal<string>(cachedKey, out _), "a:-prefixed key must be cached.");

            // Read once. Outside every configured prefix, so it is neither stored in L1 nor tracked by the server.
            await cache.SetAsync(uncachedKey, "v1");
            Assert.Equal("v1", await cache.GetAsync<string>(uncachedKey));
            Assert.False(cache.TryGetLocal<string>(uncachedKey, out _), "b:-prefixed key must never be cached.");

            var invalidationsBeforeCachedWrite = cache.Statistics.Invalidations;
            RedisCli.Standalone("SET", cachedKey, "v2");
            var evicted = await Poll.UntilAsync(() => !cache.TryGetLocal<string>(cachedKey, out _), TimeSpan.FromSeconds(5));
            Assert.True(evicted, "external write to the cached prefix did not evict.");

            // The eviction becomes visible one statement BEFORE the counter moves: the invalidation handler marks the
            // in-flight tracker, removes the entry, and only then counts it. Snapshotting the counter as soon as the
            // entry is gone can therefore capture it pre-increment, and this key's own invalidation then lands inside
            // the window below and is blamed on the untracked key. Wait for it to be counted first.
            Assert.True(
                await Poll.UntilAsync(() => cache.Statistics.Invalidations > invalidationsBeforeCachedWrite, TimeSpan.FromSeconds(5)),
                "the eviction of the cached key was never counted as an invalidation.");

            var invalidationsBefore = cache.Statistics.Invalidations;
            RedisCli.Standalone("SET", uncachedKey, "v2");
            var sawInvalidation = await Poll.UntilAsync(
                () => cache.Statistics.Invalidations > invalidationsBefore, TimeSpan.FromSeconds(1));
            Assert.False(sawInvalidation,
                "a write to a key outside KeyPrefixes produced an invalidation: the read was tracked although it was sent with CLIENT CACHING NO.");
            Assert.False(cache.TryGetLocal<string>(uncachedKey, out _), "still never cached.");
            Assert.Equal("v2", await cache.GetAsync<string>(uncachedKey));
        }
        finally
        {
            await handle.DisposeAsync();
            RedisCli.Standalone("DEL", cachedKey, uncachedKey);
        }
    }

    /// <summary>
    /// The server tracks one-shot per key and does not know what L1 holds, so an invalidation for a key that is no
    /// longer in L1 (evicted locally, aged out, dropped by the size limit) is routine. It must be counted and shrugged
    /// off, never treated as unexpected, and the next read must see the new value.
    /// </summary>
    [Fact]
    public async Task InvalidationForAKeyNotInL1IsHarmless()
    {
        var handle = await EdgeCaseSupport.BuildAsync(StandaloneCacheFixture.ConnectionString);
        var key = TestHelpers.Key("evicted-then-written");
        try
        {
            var cache = handle.Cache;
            await cache.SetAsync(key, "v1");
            Assert.True(await TestHelpers.ReadUntilCachedAsync(cache, key, "v1"), "the key was not cached.");

            // Gone from L1, still tracked on the server.
            cache.EvictLocal(key);
            Assert.False(cache.TryGetLocal<string>(key, out _));

            var invalidationsBefore = cache.Statistics.Invalidations;
            RedisCli.Standalone("SET", key, "v2");
            Assert.True(await Poll.UntilAsync(() => cache.Statistics.Invalidations > invalidationsBefore, TimeSpan.FromSeconds(5)),
                "the server did not push an invalidation for a tracked key that L1 no longer holds.");
            Assert.False(cache.TryGetLocal<string>(key, out _));
            Assert.Equal("v2", await cache.GetAsync<string>(key));
            Assert.True(cache.TryGetLocal<string>(key, out _), "the key was not cached again after the invalidation.");
        }
        finally
        {
            await handle.DisposeAsync();
            RedisCli.Standalone("DEL", key);
        }
    }
}
