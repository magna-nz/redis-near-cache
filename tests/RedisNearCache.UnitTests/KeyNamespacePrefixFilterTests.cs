using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using RedisNearCache.Tracking;
using Xunit;
using Facade = RedisNearCache.Caching.RedisNearCache;

namespace RedisNearCache.UnitTests;

/// <summary>
/// <see cref="RedisNearCacheOptions.KeyPrefixes"/> are RELATIVE to
/// <see cref="RedisNearCacheOptions.KeyNamespace"/>, at the facade's own filter: with the namespace
/// <c>"app1:"</c> and the prefix <c>"user:"</c>, the caller's <c>user:1</c> (Redis key <c>app1:user:1</c>) is
/// cacheable and its <c>order:1</c> is not. The filter runs on the FULL key against
/// <see cref="RedisNearCacheOptions.EffectiveKeyPrefixes"/>, which is also what the Broadcast tracker arms the
/// server with, so the two cannot drift apart.
/// </summary>
/// <remarks>
/// <see cref="TrackingMode.Broadcast"/>, so that the key outside the prefixes is read with a plain <c>GET</c>:
/// the <c>Redirect</c> path would send <c>MULTI</c> / <c>CLIENT CACHING NO</c> / <c>GET</c> / <c>EXEC</c>, which
/// <see cref="FakeMultiplexer"/> does not model (deliberately - it throws on anything it does not model). What
/// changes with the mode is only how the read is sent; which keys are cacheable is this filter either way, and the
/// Redirect form is proved against a real Redis in
/// <c>tests/RedisNearCache.Tests/Namespacing/KeyNamespaceRedirectTests.cs</c>.
/// </remarks>
public class KeyNamespacePrefixFilterTests
{
    private const string Namespace = "app1:";

    private static async Task<(FakeMultiplexer Mux, Facade Cache)> StartAsync(string? keyNamespace, params string[] keyPrefixes)
    {
        var mux = new FakeMultiplexer("rnc-unit-" + Guid.NewGuid().ToString("N"));
        mux.Add(7000, isReplica: false);
        var connection = FakeRedis.Connection(mux);
        var armer = new TrackingArmer(connection, NullLogger<TrackingArmer>.Instance, Timeout.InfiniteTimeSpan);
        var options = new RedisNearCacheOptions { KeyNamespace = keyNamespace, TrackingMode = TrackingMode.Broadcast };
        foreach (var prefix in keyPrefixes) options.KeyPrefixes.Add(prefix);
        var cache = new Facade(connection, armer, new SilentListener(), Options.Create(options), NullLogger<Facade>.Instance);
        await cache.Ready;
        return (mux, cache);
    }

    [Fact]
    public async Task AKeyInsideThePrefixIsCachedAndOneOutsideItIsAnsweredButNotCached()
    {
        var (mux, cache) = await StartAsync(Namespace, "user:");
        await using var lifetime = cache;

        Assert.Equal("v1", await cache.GetAsync<string>("user:1"));
        Assert.True(cache.TryGetLocal<string>("user:1", out _),
            "user:1 is app1:user:1, which matches the effective prefix app1:user:, so it must be cached.");

        Assert.Equal("v1", await cache.GetAsync<string>("order:1"));
        Assert.False(cache.TryGetLocal<string>("order:1", out _),
            "order:1 is app1:order:1, which is outside app1:user:, so it must never be cached.");

        // Both were really read from Redis, under their full keys.
        Assert.Equal(1, mux.StringGetCallsFor(Namespace + "user:1"));
        Assert.Equal(1, mux.StringGetCallsFor(Namespace + "order:1"));
    }

    /// <summary>
    /// The prefix is matched against the FULL key, so the caller's key must not be matched against the un-prefixed
    /// list: a caller key that happens to start with the namespace (<c>app1:user:1</c>, meant as a key in its own
    /// right) is <c>app1:app1:user:1</c> in Redis and is therefore outside the prefixes.
    /// </summary>
    [Fact]
    public async Task ThePrefixIsMatchedAgainstTheFullKeyNotTheCallersKey()
    {
        var (_, cache) = await StartAsync(Namespace, "user:");
        await using var lifetime = cache;

        Assert.Equal("v1", await cache.GetAsync<string>(Namespace + "user:1"));

        Assert.False(cache.TryGetLocal<string>(Namespace + "user:1", out _),
            "the caller's key is namespaced once, so app1:app1:user:1 is outside app1:user: and must not be cached.");
    }

    /// <summary>
    /// A namespace with NO prefixes makes the namespace itself the only prefix, so a caller key is always inside it
    /// and everything the caller reads is cacheable - the same as an un-namespaced cache with no prefixes.
    /// </summary>
    [Fact]
    public async Task ANamespaceWithNoPrefixesStillCachesEverythingTheCallerReads()
    {
        var (_, cache) = await StartAsync(Namespace);
        await using var lifetime = cache;

        Assert.Equal("v1", await cache.GetAsync<string>("anything:1"));

        Assert.True(cache.TryGetLocal<string>("anything:1", out _),
            "with no KeyPrefixes every key the caller reads is inside the namespace and must be cached.");
    }

    /// <summary>The same options WITHOUT a namespace: unchanged behaviour, and the control for the first test above.</summary>
    [Fact]
    public async Task WithoutANamespaceThePrefixesAreMatchedAsTheyAlwaysWere()
    {
        var (_, cache) = await StartAsync(keyNamespace: null, "user:");
        await using var lifetime = cache;

        Assert.Equal("v1", await cache.GetAsync<string>("user:1"));
        Assert.True(cache.TryGetLocal<string>("user:1", out _));

        Assert.Equal("v1", await cache.GetAsync<string>("order:1"));
        Assert.False(cache.TryGetLocal<string>("order:1", out _));
    }
}
