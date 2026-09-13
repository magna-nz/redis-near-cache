using Xunit;

namespace RedisNearCache.Tests.EdgeCases;

/// <summary>
/// <see cref="RedisNearCacheOptions.KeyPrefixes"/> only decides what gets stored in L1
/// (RedisNearCache.MatchesPrefixes); it has no effect on what the Redis server tracks, because every key read
/// through the private connection is tracked regardless of prefix (design: "only keys read through
/// RedisNearCache are tracked, because only its connection is tracked; prefix opt-in further limits what is
/// stored in L1"). So a key outside every configured prefix is never cached, but the server still remembers
/// that this connection read it and still pushes an invalidation for it when someone writes it - the listener
/// must shrug that off (nothing to remove from L1) rather than treat it as unexpected.
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

            // Read once so the server starts tracking it on our connection, even though it will never be
            // stored in L1 (outside every configured prefix).
            await cache.SetAsync(uncachedKey, "v1");
            Assert.Equal("v1", await cache.GetAsync<string>(uncachedKey));
            Assert.False(cache.TryGetLocal<string>(uncachedKey, out _), "b:-prefixed key must never be cached.");

            RedisCli.Standalone("SET", cachedKey, "v2");
            var evicted = await Poll.UntilAsync(() => !cache.TryGetLocal<string>(cachedKey, out _), TimeSpan.FromSeconds(5));
            Assert.True(evicted, "external write to the cached prefix did not evict.");

            var invalidationsBefore = cache.Statistics.Invalidations;
            RedisCli.Standalone("SET", uncachedKey, "v2");
            var sawInvalidation = await Poll.UntilAsync(
                () => cache.Statistics.Invalidations > invalidationsBefore, TimeSpan.FromSeconds(5));
            Assert.True(sawInvalidation,
                "a write to the uncached prefix should still produce an invalidation message for the server-tracked key.");
            Assert.False(cache.TryGetLocal<string>(uncachedKey, out _), "still never cached after its own invalidation.");
            Assert.Equal("v2", await cache.GetAsync<string>(uncachedKey));
        }
        finally
        {
            await handle.DisposeAsync();
            RedisCli.Standalone("DEL", cachedKey, uncachedKey);
        }
    }
}
