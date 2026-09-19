using RedisNearCache.Tests.Chaos;
using RedisNearCache.Tests.EdgeCases;
using Xunit;

namespace RedisNearCache.Tests.Enterprise;

/// <summary>
/// <see cref="RedisNearCacheOptions.KeyNamespace"/> through the Redis Enterprise proxy, where
/// <see cref="TrackingMode.Broadcast"/> is the only mode that works and its cost is what the namespace addresses:
/// with no <see cref="RedisNearCacheOptions.KeyPrefixes"/> the server is armed with the namespace itself rather than
/// the whole keyspace. Skipped unless <c>RNC_ENTERPRISE_REDIS</c> is set, like the rest of the Enterprise suite.
/// </summary>
public class EnterpriseKeyNamespaceTests
{
    private static string ConnectionString => Environment.GetEnvironmentVariable("RNC_ENTERPRISE_REDIS")!;

    [EnterpriseFact]
    public async Task ANamespaceAloneScopesBroadcastTrackingToItselfThroughTheProxy()
    {
        var keyNamespace = $"ent-ns:{Guid.NewGuid():N}:";
        var inside = "user:" + Guid.NewGuid().ToString("N");
        var control = "user:" + Guid.NewGuid().ToString("N");
        var outside = $"ent-other:{Guid.NewGuid():N}";
        EdgeCaseProvider? handle = null;
        ForeignClient? foreign = null;
        try
        {
            handle = await EdgeCaseSupport.BuildAsync(ConnectionString, o =>
            {
                o.TrackingMode = TrackingMode.Broadcast;
                o.KeyNamespace = keyNamespace;   // and no KeyPrefixes: the namespace is the BCAST PREFIX
            });
            foreign = await ForeignClient.ConnectAsync(ConnectionString);
            var cache = handle.Cache;

            // Written through the cache with the caller's key; Redis holds it under the namespace.
            await cache.SetAsync(inside, "v1");
            Assert.Equal("v1", (string?)await foreign.Db.StringGetAsync(keyNamespace + inside));
            Assert.True((await foreign.Db.StringGetAsync(inside)).IsNull, "the value also landed at the bare key.");
            Assert.True(await TestHelpers.ReadUntilCachedAsync(cache, inside, "v1", TimeSpan.FromSeconds(15)), "the namespaced key was not cached through the proxy.");

            // A write outside the namespace must not reach this cache at all...
            var invalidations = cache.Statistics.Invalidations;
            await foreign.Db.StringSetAsync(outside, "x");
            // ...which only means something if a write INSIDE it, made afterwards on the same connection, does.
            await foreign.Db.StringSetAsync(keyNamespace + control, "c1");
            Assert.True(await TestHelpers.ReadUntilCachedAsync(cache, control, "c1", TimeSpan.FromSeconds(15)), "the control key was not cached.");
            await foreign.Db.StringSetAsync(keyNamespace + control, "c2");
            Assert.True(await Poll.UntilAsync(() => !cache.TryGetLocal<string>(control, out _), TimeSpan.FromSeconds(15)), "a write inside the namespace did not invalidate.");
            Assert.True(cache.TryGetLocal<string>(inside, out var still) && still == "v1", "a write outside the namespace evicted a namespaced entry.");
            Assert.True(cache.Statistics.Invalidations - invalidations >= 1, "no invalidation arrived for the control key.");

            // And a foreign write to the FULL key of the cached entry does evict it.
            await foreign.Db.StringSetAsync(keyNamespace + inside, "v2");
            Assert.True(await TestHelpers.ReadUntilCachedAsync(cache, inside, "v2", TimeSpan.FromSeconds(15)), "a foreign write to the namespaced key did not evict it.");
        }
        finally
        {
            if (foreign is not null)
            {
                await foreign.Db.KeyDeleteAsync([keyNamespace + inside, keyNamespace + control, outside]);
                await foreign.DisposeAsync();
            }

            if (handle is not null) await handle.DisposeAsync();
        }
    }
}
