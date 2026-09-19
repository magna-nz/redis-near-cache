using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using RedisNearCache.Internal;
using RedisNearCache.Tracking;
using StackExchange.Redis;
using Xunit;
using Facade = RedisNearCache.Caching.RedisNearCache;

namespace RedisNearCache.UnitTests;

/// <summary>
/// The library's own facade over <see cref="FakeRedis"/> with <see cref="RedisNearCacheOptions.KeyNamespace"/> set.
/// The rule under test is that the caller's key becomes <c>namespace + key</c> exactly once, at the facade's edge,
/// and that everything below - Redis, L1, the in-flight tracker, and the invalidations the server sends back - is
/// then talking about the full key. So: which key each command carried, which key the listener has to name to evict
/// a local copy, and that the caller's own keys come back out of the multi-key read.
/// </summary>
/// <remarks>
/// <see cref="FakeMultiplexer"/> serves ONE stored value for every key, so nothing here can tell one key's value
/// from another's; the assertions are about which key a command carried
/// (<see cref="FakeMultiplexer.StringGetCallsFor"/>, <see cref="FakeMultiplexer.LastSetKey"/>,
/// <see cref="FakeMultiplexer.LastDeletedKey"/>). Values under separate namespaces really not colliding is proved
/// against a real Redis in <c>tests/RedisNearCache.Tests/Namespacing/</c>.
/// </remarks>
public class KeyNamespaceFacadeTests
{
    private const string Namespace = "ns:";
    private const string Key = "k";
    private const string FullKey = Namespace + Key;

    /// <summary>A listener a test can raise invalidations on, which <see cref="SilentListener"/> deliberately cannot.</summary>
    private sealed class RaisingListener : IInvalidationListener
    {
        public event Action<string>? KeyInvalidated;
        public event Action? FlushAll;

        public void Raise(string key) => KeyInvalidated?.Invoke(key);
        public void RaiseFlush() => FlushAll?.Invoke();

        public Task StartAsync(CancellationToken cancellationToken) => Task.CompletedTask;
        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }

    private static async Task<(FakeMultiplexer Mux, Facade Cache, RaisingListener Listener)> StartAsync(
        string? keyNamespace = Namespace,
        Action<RedisNearCacheOptions>? configure = null)
    {
        var mux = new FakeMultiplexer("rnc-unit-" + Guid.NewGuid().ToString("N"));
        mux.Add(7000, isReplica: false);
        var connection = FakeRedis.Connection(mux);
        var armer = new TrackingArmer(connection, NullLogger<TrackingArmer>.Instance, Timeout.InfiniteTimeSpan);
        var options = new RedisNearCacheOptions { KeyNamespace = keyNamespace };
        configure?.Invoke(options);
        var listener = new RaisingListener();
        var cache = new Facade(connection, armer, listener, Options.Create(options), NullLogger<Facade>.Instance);
        await cache.Ready;
        return (mux, cache, listener);
    }

    // --- reads ------------------------------------------------------------------------------------------------

    [Fact]
    public async Task GetAsyncReadsTheNamespacedKeyAndNeverTheBareOne()
    {
        var (mux, cache, _) = await StartAsync();
        await using var lifetime = cache;

        Assert.Equal("v1", await cache.GetAsync<string>(Key));

        Assert.Equal(1, mux.StringGetCallsFor(FullKey));
        Assert.Equal(0, mux.StringGetCallsFor(Key));
    }

    [Fact]
    public async Task GetBytesAsyncReadsTheNamespacedKeyAndNeverTheBareOne()
    {
        var (mux, cache, _) = await StartAsync();
        await using var lifetime = cache;

        Assert.Equal("v1"u8.ToArray(), await cache.GetBytesAsync(Key));

        Assert.Equal(1, mux.StringGetCallsFor(FullKey));
        Assert.Equal(0, mux.StringGetCallsFor(Key));
    }

    [Fact]
    public async Task AReadCachesUnderTheFullKeyAndTheSecondReadTouchesNoRedis()
    {
        var (mux, cache, _) = await StartAsync();
        await using var lifetime = cache;

        Assert.Equal("v1", await cache.GetAsync<string>(Key));
        Assert.True(cache.TryGetLocal<string>(Key, out var local), "the read did not populate L1 under the caller's key.");
        Assert.Equal("v1", local);

        Assert.Equal("v1", await cache.GetAsync<string>(Key));

        Assert.Equal(1, mux.StringGetCallsFor(FullKey));
        Assert.Equal(1, cache.Statistics.Hits);
    }

    /// <summary>
    /// The caller peeks with its own key; nothing else does. A cache that stored under the bare key would make the
    /// first of these pass and the second fail, and one that peeked without translating would do the opposite.
    /// </summary>
    [Fact]
    public async Task TryGetLocalTakesTheCallersKeyNotTheFullOne()
    {
        var (_, cache, _) = await StartAsync();
        await using var lifetime = cache;
        Assert.Equal("v1", await cache.GetAsync<string>(Key));

        Assert.True(cache.TryGetLocal<string>(Key, out _));
        Assert.False(cache.TryGetLocal<string>(FullKey, out _),
            "the caller's key is already namespaced once; peeking with the full key must not find the entry again.");
    }

    [Fact]
    public async Task EvictLocalTakesTheCallersKey()
    {
        var (_, cache, _) = await StartAsync();
        await using var lifetime = cache;
        Assert.Equal("v1", await cache.GetAsync<string>(Key));
        Assert.True(cache.TryGetLocal<string>(Key, out _), "precondition: the key is cached.");

        cache.EvictLocal(FullKey);
        Assert.True(cache.TryGetLocal<string>(Key, out _),
            "evicting ns+key (which becomes ns+ns+key) must not have dropped the entry.");

        cache.EvictLocal(Key);
        Assert.False(cache.TryGetLocal<string>(Key, out _), "EvictLocal with the caller's own key did not evict.");
    }

    [Fact]
    public async Task GetManyAsyncReadsNamespacedKeysAndAnswersWithTheCallersKeys()
    {
        var (mux, cache, _) = await StartAsync();
        await using var lifetime = cache;
        IRedisNearCache viaInterface = cache;

        var result = await viaInterface.GetManyAsync<string>(["a", "b"]);

        Assert.Equal(new[] { "a", "b" }, result.Keys.OrderBy(k => k, StringComparer.Ordinal));
        Assert.Equal("v1", result["a"]);
        Assert.Equal(1, mux.StringGetCallsFor(Namespace + "a"));
        Assert.Equal(1, mux.StringGetCallsFor(Namespace + "b"));
        Assert.Equal(0, mux.StringGetCallsFor("a"));
        Assert.Equal(0, mux.StringGetCallsFor("b"));
    }

    [Fact]
    public async Task GetManyBytesAsyncReadsNamespacedKeysAndAnswersWithTheCallersKeys()
    {
        var (mux, cache, _) = await StartAsync();
        await using var lifetime = cache;
        IRedisNearCache viaInterface = cache;

        var result = await viaInterface.GetManyBytesAsync(["a", "b"]);

        Assert.Equal(new[] { "a", "b" }, result.Keys.OrderBy(k => k, StringComparer.Ordinal));
        Assert.Equal("v1"u8.ToArray(), result["a"]);
        Assert.Equal(1, mux.StringGetCallsFor(Namespace + "a"));
        Assert.Equal(0, mux.StringGetCallsFor("a"));
    }

    // --- writes -----------------------------------------------------------------------------------------------

    [Fact]
    public async Task SetAsyncWritesTheNamespacedKey()
    {
        var (mux, cache, _) = await StartAsync();
        await using var lifetime = cache;

        await cache.SetAsync(Key, "v2");

        Assert.Equal(FullKey, mux.LastSetKey);
        Assert.Equal(1, mux.StringSetCallsFor(FullKey));
        Assert.Equal(0, mux.StringSetCallsFor(Key));
    }

    [Fact]
    public async Task SetBytesAsyncWritesTheNamespacedKey()
    {
        var (mux, cache, _) = await StartAsync();
        await using var lifetime = cache;

        await cache.SetBytesAsync(Key, new byte[] { 1, 2, 3 });

        Assert.Equal(FullKey, mux.LastSetKey);
        Assert.Equal(0, mux.StringSetCallsFor(Key));
    }

    [Fact]
    public async Task AConditionalSetAsyncWritesTheNamespacedKey()
    {
        var (mux, cache, _) = await StartAsync();
        await using var lifetime = cache;
        mux.StoredValue = RedisValue.Null;

        Assert.True(await cache.SetAsync(Key, "v2", When.NotExists));

        Assert.Equal(FullKey, mux.LastSetKey);
        Assert.Equal(When.NotExists, mux.LastSetWhen);
        Assert.Equal(0, mux.StringSetCallsFor(Key));
    }

    [Fact]
    public async Task AConditionalSetBytesAsyncWritesTheNamespacedKey()
    {
        var (mux, cache, _) = await StartAsync();
        await using var lifetime = cache;
        mux.StoredValue = RedisValue.Null;

        Assert.True(await cache.SetBytesAsync(Key, new byte[] { 1 }, When.NotExists));

        Assert.Equal(FullKey, mux.LastSetKey);
        Assert.Equal(0, mux.StringSetCallsFor(Key));
    }

    [Fact]
    public async Task RemoveAsyncDeletesTheNamespacedKey()
    {
        var (mux, cache, _) = await StartAsync();
        await using var lifetime = cache;
        mux.StoredValue = "v1";

        Assert.True(await cache.RemoveAsync(Key));

        Assert.Equal(1, mux.KeyDeleteCalls);
        Assert.Equal(FullKey, mux.LastDeletedKey);
    }

    /// <summary>A write through the cache still drops the local copy - the eviction is keyed by the full key too.</summary>
    [Fact]
    public async Task AWriteEvictsTheLocalCopyOfTheCallersKey()
    {
        var (_, cache, _) = await StartAsync();
        await using var lifetime = cache;
        Assert.Equal("v1", await cache.GetAsync<string>(Key));
        Assert.True(cache.TryGetLocal<string>(Key, out _), "precondition: the key is cached.");

        await cache.SetAsync(Key, "v2");

        Assert.False(cache.TryGetLocal<string>(Key, out _), "a write through the cache did not evict the local copy.");
    }

    // --- invalidations ----------------------------------------------------------------------------------------

    /// <summary>
    /// The server names the key it holds, i.e. the FULL one; the invalidation path translates nothing. The two
    /// negatives are what proves it: an invalidation for the caller's bare key, or for the same key under another
    /// namespace, belongs to somebody else and must leave this entry alone. The positive at the end is the control
    /// that the listener is really wired up and that the entry was evictable all along.
    /// </summary>
    [Fact]
    public async Task OnlyAnInvalidationForTheFullKeyEvicts()
    {
        var (_, cache, listener) = await StartAsync();
        await using var lifetime = cache;
        Assert.Equal("v1", await cache.GetAsync<string>(Key));
        Assert.True(cache.TryGetLocal<string>(Key, out _), "precondition: the key is cached.");
        var invalidationsBefore = cache.Statistics.Invalidations;

        listener.Raise(Key);
        Assert.True(cache.TryGetLocal<string>(Key, out _), "an invalidation for the BARE key evicted a namespaced entry.");

        listener.Raise("other:" + Key);
        Assert.True(cache.TryGetLocal<string>(Key, out _), "an invalidation for another namespace's key evicted this one.");

        // The two above were delivered (the counter moved), they just matched nothing - not a listener that was never
        // subscribed, which would make the assertions above pass for the wrong reason.
        Assert.Equal(invalidationsBefore + 2, cache.Statistics.Invalidations);

        listener.Raise(FullKey);
        Assert.False(cache.TryGetLocal<string>(Key, out _), "an invalidation for the full key did not evict.");
        Assert.Equal(invalidationsBefore + 3, cache.Statistics.Invalidations);
    }

    /// <summary>A FLUSHDB drops everything regardless of namespace; nothing about that changes.</summary>
    [Fact]
    public async Task AFlushStillDropsANamespacedEntry()
    {
        var (_, cache, listener) = await StartAsync();
        await using var lifetime = cache;
        Assert.Equal("v1", await cache.GetAsync<string>(Key));
        Assert.True(cache.TryGetLocal<string>(Key, out _), "precondition: the key is cached.");

        listener.RaiseFlush();

        Assert.False(cache.TryGetLocal<string>(Key, out _));
    }

    // --- null keys --------------------------------------------------------------------------------------------

    /// <summary>
    /// With a namespace a null key must be refused rather than quietly becoming the namespace itself - which would
    /// read, write or delete a real key shared by every caller that made the same mistake.
    /// </summary>
    [Fact]
    public async Task EveryMemberTakingAKeyRefusesNullOnceANamespaceIsSet()
    {
        var (mux, cache, _) = await StartAsync();
        await using var lifetime = cache;

        await Assert.ThrowsAsync<ArgumentNullException>(() => cache.GetAsync<string>(null!).AsTask());
        await Assert.ThrowsAsync<ArgumentNullException>(() => cache.GetBytesAsync(null!).AsTask());
        await Assert.ThrowsAsync<ArgumentNullException>(() => cache.SetAsync(null!, "v1").AsTask());
        await Assert.ThrowsAsync<ArgumentNullException>(() => cache.SetAsync(null!, "v1", When.Always).AsTask());
        await Assert.ThrowsAsync<ArgumentNullException>(() => cache.SetBytesAsync(null!, new byte[] { 1 }).AsTask());
        await Assert.ThrowsAsync<ArgumentNullException>(() => cache.SetBytesAsync(null!, new byte[] { 1 }, When.Always).AsTask());
        await Assert.ThrowsAsync<ArgumentNullException>(() => cache.RemoveAsync(null!).AsTask());
        Assert.Throws<ArgumentNullException>(() => cache.EvictLocal(null!));
        Assert.Throws<ArgumentNullException>(() => cache.TryGetLocal<string>(null!, out _));

        // Nothing reached Redis under the namespace alone.
        Assert.Equal(0, mux.StringGetCallsFor(Namespace));
        Assert.Equal(0, mux.StringSetCallsFor(Namespace));
        Assert.Equal(0, mux.KeyDeleteCalls);
    }
}
