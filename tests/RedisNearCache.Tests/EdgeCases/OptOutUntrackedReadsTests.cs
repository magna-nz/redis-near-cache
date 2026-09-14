using Xunit;

namespace RedisNearCache.Tests.EdgeCases;

/// <summary>
/// The connection is armed with <c>CLIENT TRACKING ON REDIRECT &lt;id&gt; OPTOUT NOLOOP</c>. A key outside
/// every configured <see cref="RedisNearCacheOptions.KeyPrefixes"/> (only meaningful once KeyPrefixes is
/// non-empty) is read as <c>MULTI</c> / <c>CLIENT CACHING NO</c> / <c>GET</c> / <c>EXEC</c>, so the server
/// never tracks it and a later write to it pushes no invalidation. A key that matches a prefix is still read
/// with a plain GET and tracked as before.
/// </summary>
public class OptOutModeArmingTests
{
    [Fact]
    public async Task ConnectionIsArmedInOptOutMode()
    {
        var handle = await EdgeCaseSupport.BuildAsync(StandaloneCacheFixture.ConnectionString, o => o.KeyPrefixes.Add("p:"));
        try
        {
            var endpoint = handle.Multiplexer.GetEndPoints()[0];
            var server = handle.Multiplexer.GetServer(endpoint);

            // Runs on the same interactive connection the armer armed.
            var info = await server.ExecuteAsync("CLIENT", "TRACKINGINFO");
            var fields = info.ToDictionary();

            var flags = ((string[])fields["flags"]!).Select(f => f.ToLowerInvariant()).ToArray();
            Assert.Contains("on", flags);
            Assert.Contains("optout", flags);
            Assert.Contains("noloop", flags);

            var redirect = (long)fields["redirect"];
            Assert.True(handle.Armer.RedirectTargets.TryGetValue(endpoint, out var armedRedirect),
                $"{endpoint} is not an armed master: {string.Join(",", handle.Armer.RedirectTargets.Keys)}");
            Assert.Equal(armedRedirect, redirect);
        }
        finally
        {
            await handle.DisposeAsync();
        }
    }
}

public class OptOutUntrackedReadsTests
{
    [Fact]
    public async Task ReadOutsidePrefixesIsNotTrackedButReadInsideIs()
    {
        var qKey = "q:" + TestHelpers.Key("q");
        var pKey = "p:" + TestHelpers.Key("p");
        RedisCli.Standalone("SET", qKey, "v1");
        RedisCli.Standalone("SET", pKey, "v1");

        var handle = await EdgeCaseSupport.BuildAsync(StandaloneCacheFixture.ConnectionString, o => o.KeyPrefixes.Add("p:"));
        try
        {
            var server = handle.Multiplexer.GetServer(handle.Multiplexer.GetEndPoints()[0]);
            var cache = handle.Cache;

            var multiBefore = TestHelpers.CommandCalls(server, "multi");
            var execBefore = TestHelpers.CommandCalls(server, "exec");

            Assert.Equal("v1", await cache.GetAsync<string>(qKey));
            Assert.False(cache.TryGetLocal<string>(qKey, out _), "a key outside KeyPrefixes must never be cached in L1.");

            Assert.Equal(multiBefore + 1, TestHelpers.CommandCalls(server, "multi"));
            Assert.Equal(execBefore + 1, TestHelpers.CommandCalls(server, "exec"));

            var invalidationsBefore = cache.Statistics.Invalidations;
            RedisCli.Standalone("SET", qKey, "v2");
            var sawInvalidation = await Poll.UntilAsync(
                () => cache.Statistics.Invalidations > invalidationsBefore, TimeSpan.FromMilliseconds(1000));
            Assert.False(sawInvalidation, "a key outside KeyPrefixes must not be tracked, but a write to it produced an invalidation.");

            Assert.Equal("v2", await cache.GetAsync<string>(qKey)); // still correct, just never cached (and another transaction)
            Assert.Equal(multiBefore + 2, TestHelpers.CommandCalls(server, "multi"));

            // Positive control: a prefixed key is still tracked and cached as before, with a plain GET.
            var multiBeforePrefixed = TestHelpers.CommandCalls(server, "multi");
            Assert.True(await TestHelpers.ReadUntilCachedAsync(cache, pKey, "v1"), "the p:-prefixed key was not cached.");
            Assert.Equal(multiBeforePrefixed, TestHelpers.CommandCalls(server, "multi")); // unchanged: that read was a plain GET.

            RedisCli.Standalone("SET", pKey, "v2");
            var pInvalidated = await Poll.UntilAsync(
                () => cache.Statistics.Invalidations > invalidationsBefore, TimeSpan.FromSeconds(3));
            Assert.True(pInvalidated, "a write to a tracked, prefixed key should have produced an invalidation.");

            var evicted = await Poll.UntilAsync(() => !cache.TryGetLocal<string>(pKey, out _), TimeSpan.FromSeconds(3));
            Assert.True(evicted, "the invalidation for the prefixed key should have evicted it from L1.");
        }
        finally
        {
            await handle.DisposeAsync();
            RedisCli.Standalone("DEL", qKey, pKey);
        }
    }

    [Fact]
    public async Task UntrackedReadRoutesToTheRightNodeOnACluster()
    {
        var qKey = "q:" + TestHelpers.Key("q");
        var pKey = "p:" + TestHelpers.Key("p");
        RedisCli.Cluster(7100, "SET", qKey, "v1");
        RedisCli.Cluster(7100, "SET", pKey, "v1");

        var handle = await EdgeCaseSupport.BuildAsync(ClusterCacheFixture.ConnectionString, o => o.KeyPrefixes.Add("p:"));
        try
        {
            var cache = handle.Cache;

            // No exception: a MULTI/EXEC transaction on a cluster must be routed by the key's own slot.
            Assert.Equal("v1", await cache.GetAsync<string>(qKey));
            Assert.False(cache.TryGetLocal<string>(qKey, out _), "a key outside KeyPrefixes must never be cached in L1.");

            var invalidationsBefore = cache.Statistics.Invalidations;
            RedisCli.Cluster(7100, "SET", qKey, "v2");
            var sawInvalidation = await Poll.UntilAsync(
                () => cache.Statistics.Invalidations > invalidationsBefore, TimeSpan.FromMilliseconds(1000));
            Assert.False(sawInvalidation, "a key outside KeyPrefixes must not be tracked, but a write to it produced an invalidation.");

            Assert.Equal("v2", await cache.GetAsync<string>(qKey));

            // Positive control.
            Assert.True(await TestHelpers.ReadUntilCachedAsync(cache, pKey, "v1"), "the p:-prefixed key was not cached.");
            RedisCli.Cluster(7100, "SET", pKey, "v2");
            var pInvalidated = await Poll.UntilAsync(
                () => cache.Statistics.Invalidations > invalidationsBefore, TimeSpan.FromSeconds(3));
            Assert.True(pInvalidated, "a write to a tracked, prefixed key should have produced an invalidation.");
        }
        finally
        {
            await handle.DisposeAsync();
            RedisCli.Cluster(7100, "DEL", qKey);
            RedisCli.Cluster(7100, "DEL", pKey);
        }
    }

    [Fact]
    public async Task WithoutKeyPrefixesNoTransactionIsUsed()
    {
        var handle = await EdgeCaseSupport.BuildAsync(StandaloneCacheFixture.ConnectionString);
        var key = TestHelpers.Key("no-prefixes");
        try
        {
            RedisCli.Standalone("SET", key, "v1");
            var server = handle.Multiplexer.GetServer(handle.Multiplexer.GetEndPoints()[0]);
            var multiBefore = TestHelpers.CommandCalls(server, "multi");

            Assert.Equal("v1", await handle.Cache.GetAsync<string>(key));
            Assert.Equal(multiBefore, TestHelpers.CommandCalls(server, "multi"));

            Assert.True(handle.Cache.TryGetLocal<string>(key, out var cached),
                "with KeyPrefixes empty every key is cacheable, so this miss should have been served with a plain GET and cached.");
            Assert.Equal("v1", cached);
        }
        finally
        {
            await handle.DisposeAsync();
            RedisCli.Standalone("DEL", key);
        }
    }
}
