using System.Reflection;
using System.Text;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using RedisNearCache.Tracking;
using Xunit;
using Facade = RedisNearCache.Caching.RedisNearCache;

namespace RedisNearCache.UnitTests;

/// <summary>
/// The regression guard for <see cref="RedisNearCacheOptions.KeyNamespace"/>: a cache that does not set it - which
/// is every cache that exists today - must put the caller's key on the wire exactly as given, and must not so much
/// as touch it on the way. The facade's private <c>FullKey</c> is the single place a key could be rewritten, so it
/// is checked directly (by reflection) as well as through the commands the fake receives.
/// </summary>
public class NoKeyNamespaceRegressionTests
{
    private const string Key = "k";

    private static async Task<(FakeMultiplexer Mux, Facade Cache)> StartAsync(string? keyNamespace)
    {
        var mux = new FakeMultiplexer("rnc-unit-" + Guid.NewGuid().ToString("N"));
        mux.Add(7000, isReplica: false);
        var connection = FakeRedis.Connection(mux);
        var armer = new TrackingArmer(connection, NullLogger<TrackingArmer>.Instance, Timeout.InfiniteTimeSpan);
        var options = new RedisNearCacheOptions { KeyNamespace = keyNamespace };
        var cache = new Facade(connection, armer, new SilentListener(), Options.Create(options), NullLogger<Facade>.Instance);
        await cache.Ready;
        return (mux, cache);
    }

    /// <summary>A string that is certainly not interned, so <see cref="Assert.Same(object?, object?)"/> means something.</summary>
    private static string Fresh(string value) => new StringBuilder(value).ToString();

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    public async Task EveryCommandCarriesTheCallersKeyUnchanged(string? keyNamespace)
    {
        var (mux, cache) = await StartAsync(keyNamespace);
        await using var lifetime = cache;

        Assert.Equal("v1", await cache.GetAsync<string>(Key));
        Assert.Equal(1, mux.StringGetCallsFor(Key));

        await cache.SetAsync(Key, "v2");
        Assert.Equal(Key, mux.LastSetKey);

        Assert.True(await cache.RemoveAsync(Key));
        Assert.Equal(Key, mux.LastDeletedKey);

        // And nothing else was ever named: with no namespace there is nothing to concatenate, so a stray "" prefix
        // (the bug an is-null check rather than an is-null-or-empty check would produce) has no way to hide here.
        Assert.Equal(1, mux.StringSetCallsFor(Key));
        Assert.Equal(1, mux.KeyDeleteCalls);
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    public async Task TheMultiKeyReadCarriesTheCallersKeysUnchanged(string? keyNamespace)
    {
        var (mux, concrete) = await StartAsync(keyNamespace);
        await using var lifetime = concrete;
        IRedisNearCache cache = concrete;

        var result = await cache.GetManyAsync<string>(["a", "b"]);

        Assert.Equal(new[] { "a", "b" }, result.Keys.OrderBy(k => k, StringComparer.Ordinal));
        Assert.Equal(1, mux.StringGetCallsFor("a"));
        Assert.Equal(1, mux.StringGetCallsFor("b"));
        Assert.Equal(2, mux.StringGetCalls);
    }

    /// <summary>
    /// Without a namespace the key is not merely equal on the way through: it is the SAME string instance, i.e. the
    /// read path does no work at all for it. This is the assertion that would fail if someone replaced the early
    /// return with an unconditional <c>(_keyNamespace ?? string.Empty) + key</c>.
    /// </summary>
    [Theory]
    [InlineData(null)]
    [InlineData("")]
    public async Task FullKeyReturnsTheSameStringInstance(string? keyNamespace)
    {
        var (_, cache) = await StartAsync(keyNamespace);
        await using var lifetime = cache;
        var fullKey = typeof(Facade).GetMethod("FullKey", BindingFlags.Instance | BindingFlags.NonPublic)
            ?? throw new InvalidOperationException("RedisNearCache.FullKey(string) not found; this test needs updating.");

        var key = Fresh("some:key");

        Assert.Same(key, fullKey.Invoke(cache, [key]));
    }

    /// <summary>
    /// And with a namespace it does concatenate - so the assertion above is a statement about the no-namespace case,
    /// not about a method that always returns its argument.
    /// </summary>
    [Fact]
    public async Task FullKeyConcatenatesOnceANamespaceIsSet()
    {
        var (_, cache) = await StartAsync("ns:");
        await using var lifetime = cache;
        var fullKey = typeof(Facade).GetMethod("FullKey", BindingFlags.Instance | BindingFlags.NonPublic)!;

        var key = Fresh("some:key");

        Assert.Equal("ns:some:key", fullKey.Invoke(cache, [key]));
    }

    /// <summary>
    /// Without a namespace a null key is not CHECKED either: it is handed on exactly as it was before the option
    /// existed, so whatever the layers below it did with a null key (they are none of this feature's business) they
    /// go on doing. With a namespace it is refused, because it would otherwise silently become the namespace
    /// itself. Asserted on <c>FullKey</c> directly: pushing a null key through a public member instead would be
    /// asserting on what <c>MemoryCache</c> and StackExchange.Redis do with one, which is not what changed here.
    /// </summary>
    [Theory]
    [InlineData(null)]
    [InlineData("")]
    public async Task FullKeyDoesNotCheckForNullWhenThereIsNoNamespace(string? keyNamespace)
    {
        var (_, cache) = await StartAsync(keyNamespace);
        await using var lifetime = cache;
        var fullKey = typeof(Facade).GetMethod("FullKey", BindingFlags.Instance | BindingFlags.NonPublic)!;

        Assert.Null(fullKey.Invoke(cache, [null]));
    }

    [Fact]
    public async Task FullKeyRefusesNullOnceANamespaceIsSet()
    {
        var (_, cache) = await StartAsync("ns:");
        await using var lifetime = cache;
        var fullKey = typeof(Facade).GetMethod("FullKey", BindingFlags.Instance | BindingFlags.NonPublic)!;

        var ex = Assert.Throws<TargetInvocationException>(() => fullKey.Invoke(cache, [null]));
        Assert.IsType<ArgumentNullException>(ex.InnerException);
    }
}
