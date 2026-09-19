using RedisNearCache.Tests.EdgeCases;
using RedisNearCache.Tracking.Broadcast;
using Xunit;

namespace RedisNearCache.Tests.Namespacing;

/// <summary>
/// <see cref="RedisNearCacheOptions.KeyNamespace"/> in <see cref="TrackingMode.Broadcast"/>, where the namespace
/// is not only a key prefix but part of what the SERVER is told: the <c>BCAST PREFIX</c> arguments come from the
/// same <c>EffectiveKeyPrefixes</c> the facade filters on, so the arm and the filter cannot disagree. The two
/// cases that matter are a namespace with prefixes (each prefix goes behind the namespace) and a namespace with
/// none (the namespace itself is armed, rather than the whole keyspace).
/// </summary>
public class KeyNamespaceBroadcastTests
{
    private static string NewNamespace() => $"ns-{Guid.NewGuid():N}:";

    private static IReadOnlyList<string> ArmedPrefixes(EdgeCaseProvider handle) =>
        Assert.IsType<BroadcastTracker>(handle.Armer).Prefixes;

    [Fact]
    public async Task ANamespaceWithPrefixesArmsTheNamespacedPrefixOnly()
    {
        var ns = NewNamespace();
        var inside = "user:" + TestHelpers.Key("bc-in");
        var insideFull = ns + inside;
        var handle = await EdgeCaseSupport.BuildAsync(StandaloneCacheFixture.ConnectionString, o =>
        {
            o.TrackingMode = TrackingMode.Broadcast;
            o.KeyNamespace = ns;
            o.KeyPrefixes.Add("user:");
        });
        try
        {
            Assert.Equal(new[] { ns + "user:" }, ArmedPrefixes(handle).ToArray());

            var cache = handle.Cache;
            RedisCli.Standalone("SET", insideFull, "v1");
            Assert.True(await TestHelpers.ReadUntilCachedAsync(cache, inside, "v1"), "the prefixed key was never cached.");

            RedisCli.Standalone("SET", insideFull, "v2");

            Assert.True(await Poll.UntilAsync(() => !cache.TryGetLocal<string>(inside, out _), TimeSpan.FromSeconds(5)),
                "a foreign write to the namespaced, prefixed key did not evict it.");
            Assert.Equal("v2", await cache.GetAsync<string>(inside));
        }
        finally
        {
            await handle.DisposeAsync();
            RedisCli.Standalone("DEL", insideFull);
        }
    }

    /// <summary>
    /// Empty <see cref="RedisNearCacheOptions.KeyPrefixes"/> under a namespace arms the NAMESPACE, not the whole
    /// keyspace. The negative (a write outside the namespace produces no invalidation for this cache) is what says
    /// the arm is really scoped; without the namespace the same registration would be armed with no prefix at all
    /// and that write would arrive. The positive control follows it.
    /// </summary>
    [Fact]
    public async Task ANamespaceWithNoPrefixesArmsTheNamespaceItself()
    {
        var ns = NewNamespace();
        var inside = TestHelpers.Key("bc-empty-in");
        var insideFull = ns + inside;
        var outside = TestHelpers.Key("bc-empty-out"); // no namespace at all: outside the armed prefix
        var handle = await EdgeCaseSupport.BuildAsync(StandaloneCacheFixture.ConnectionString, o =>
        {
            o.TrackingMode = TrackingMode.Broadcast;
            o.KeyNamespace = ns;
            // KeyPrefixes deliberately left empty.
        });
        try
        {
            Assert.Equal(new[] { ns }, ArmedPrefixes(handle).ToArray());

            var cache = handle.Cache;
            RedisCli.Standalone("SET", insideFull, "v1");
            Assert.True(await TestHelpers.ReadUntilCachedAsync(cache, inside, "v1"), "the key inside the namespace was never cached.");

            var invalidationsBefore = cache.Statistics.Invalidations;

            // Outside the namespace: under an un-scoped BCAST arm this write would be broadcast to this client.
            RedisCli.Standalone("SET", outside, "v1");
            RedisCli.Standalone("SET", outside, "v2");

            Assert.False(
                await Poll.UntilAsync(() => cache.Statistics.Invalidations > invalidationsBefore, TimeSpan.FromSeconds(2)),
                "a write outside the namespace produced an invalidation, so the BCAST arm is not scoped to it.");
            Assert.True(cache.TryGetLocal<string>(inside, out _), "the entry inside the namespace was dropped by an unrelated write.");

            // Positive control: a write INSIDE the namespace is broadcast and evicts.
            RedisCli.Standalone("SET", insideFull, "v2");
            Assert.True(await Poll.UntilAsync(() => !cache.TryGetLocal<string>(inside, out _), TimeSpan.FromSeconds(5)),
                "a write inside the namespace did not evict, so the assertion above proves nothing.");
            Assert.True(cache.Statistics.Invalidations > invalidationsBefore);
        }
        finally
        {
            await handle.DisposeAsync();
            RedisCli.Standalone("DEL", insideFull, outside);
        }
    }

    /// <summary>The control for both tests above: with no namespace the armed prefixes are KeyPrefixes, unchanged.</summary>
    [Fact]
    public async Task WithoutANamespaceTheArmedPrefixesAreKeyPrefixesUnchanged()
    {
        var withPrefixes = await EdgeCaseSupport.BuildAsync(StandaloneCacheFixture.ConnectionString, o =>
        {
            o.TrackingMode = TrackingMode.Broadcast;
            o.KeyPrefixes.Add("user:");
        });
        try
        {
            Assert.Equal(new[] { "user:" }, ArmedPrefixes(withPrefixes).ToArray());
        }
        finally
        {
            await withPrefixes.DisposeAsync();
        }

        var withNone = await EdgeCaseSupport.BuildAsync(StandaloneCacheFixture.ConnectionString,
            o => o.TrackingMode = TrackingMode.Broadcast);
        try
        {
            Assert.Empty(ArmedPrefixes(withNone));
        }
        finally
        {
            await withNone.DisposeAsync();
        }
    }
}
