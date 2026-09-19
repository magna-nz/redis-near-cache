using Xunit;

namespace RedisNearCache.UnitTests;

/// <summary>
/// The default interface method bodies of <see cref="IRedisNearCache.GetManyAsync{T}"/> and
/// <see cref="IRedisNearCache.GetManyBytesAsync"/>, exercised through fakes that implement only the members that
/// predate them (the pattern of <see cref="ConditionalWriteDefaultImplementationTests"/>): an implementation that
/// has not been updated must still compile and must get a correct multi-key read for free, routed through its own
/// single-key read and carrying the caller's token.
/// </summary>
public class GetManyDefaultImplementationTests
{
    [Fact]
    public async Task GetManyRoutesThroughTheImplementationsOwnGetAsync()
    {
        var fake = new LegacyCache();
        IRedisNearCache cache = fake;

        var result = await cache.GetManyAsync<string>(["a", "b"]);

        Assert.Equal(2, result.Count);
        Assert.Equal("typed:a", result["a"]);
        Assert.Equal("typed:b", result["b"]);
        Assert.Equal(new[] { "a", "b" }, fake.TypedReads.Order(StringComparer.Ordinal).ToArray());
        Assert.Empty(fake.ByteReads);
    }

    [Fact]
    public async Task GetManyBytesRoutesThroughTheImplementationsOwnGetBytesAsync()
    {
        var fake = new LegacyCache();
        IRedisNearCache cache = fake;

        var result = await cache.GetManyBytesAsync(["a", "b"]);

        Assert.Equal(2, result.Count);
        Assert.Equal("bytes:a"u8.ToArray(), result["a"]);
        Assert.Equal("bytes:b"u8.ToArray(), result["b"]);
        Assert.Equal(new[] { "a", "b" }, fake.ByteReads.Order(StringComparer.Ordinal).ToArray());
        Assert.Empty(fake.TypedReads);
    }

    [Fact]
    public async Task TheCallersTokenReachesEverySingleKeyRead()
    {
        var fake = new LegacyCache();
        IRedisNearCache cache = fake;
        using var cts = new CancellationTokenSource();

        await cache.GetManyAsync<string>(["a", "b"], cts.Token);
        await cache.GetManyBytesAsync(["a", "b"], cts.Token);

        Assert.Equal(4, fake.Tokens.Count);
        Assert.All(fake.Tokens, token => Assert.Equal(cts.Token, token));
    }

    [Fact]
    public async Task AMissingKeyIsPresentWithTheDefaultValue()
    {
        var fake = new LegacyCache { Missing = "b" };
        IRedisNearCache cache = fake;

        var typed = await cache.GetManyAsync<string>(["a", "b"]);
        var bytes = await cache.GetManyBytesAsync(["a", "b"]);

        Assert.True(typed.ContainsKey("b"));
        Assert.Null(typed["b"]);
        Assert.True(bytes.ContainsKey("b"));
        Assert.Null(bytes["b"]);
    }

    [Fact]
    public async Task TheArgumentRulesApplyToTheDefaultImplementationToo()
    {
        var fake = new LegacyCache();
        IRedisNearCache cache = fake;

        await Assert.ThrowsAsync<ArgumentNullException>(() => cache.GetManyAsync<string>(null!).AsTask());
        await Assert.ThrowsAsync<ArgumentNullException>(() => cache.GetManyBytesAsync(null!).AsTask());

        var typed = await Assert.ThrowsAsync<ArgumentException>(() => cache.GetManyAsync<string>(["a", null!]).AsTask());
        var raw = await Assert.ThrowsAsync<ArgumentException>(() => cache.GetManyBytesAsync(["a", null!]).AsTask());
        Assert.Equal("keys", typed.ParamName);
        Assert.Equal("keys", raw.ParamName);

        Assert.Empty(fake.TypedReads);
        Assert.Empty(fake.ByteReads);
    }

    [Fact]
    public async Task AnEmptyKeyListReturnsAnEmptyResultWithoutReading()
    {
        var fake = new LegacyCache();
        IRedisNearCache cache = fake;

        Assert.Empty(await cache.GetManyAsync<string>([]));
        Assert.Empty(await cache.GetManyBytesAsync([]));
        Assert.Empty(fake.TypedReads);
        Assert.Empty(fake.ByteReads);
    }

    /// <summary>
    /// The decorator case the interface's own remarks point callers at: a wrapper that only overrides the
    /// single-key read gets a multi-key read consistent with it, without knowing the member exists.
    /// </summary>
    [Fact]
    public async Task ADecoratorThatOnlyChangesGetAsyncGetsAConsistentGetManyAsync()
    {
        IRedisNearCache cache = new DecoratingCache(new LegacyCache());

        var result = await cache.GetManyAsync<string>(["a", "b"]);

        Assert.Equal("decorated(typed:a)", result["a"]);
        Assert.Equal("decorated(typed:b)", result["b"]);
        Assert.Equal(2, result.Count);
    }

    /// <summary>An <see cref="IRedisNearCache"/> that implements only the abstract members.</summary>
    private class LegacyCache : IRedisNearCache
    {
        private readonly object _gate = new();

        public List<string> TypedReads { get; } = [];
        public List<string> ByteReads { get; } = [];
        public List<CancellationToken> Tokens { get; } = [];

        /// <summary>The one key this fake reports as absent.</summary>
        public string? Missing { get; init; }

        public ValueTask<T?> GetAsync<T>(string key, CancellationToken cancellationToken = default)
        {
            lock (_gate)
            {
                TypedReads.Add(key);
                Tokens.Add(cancellationToken);
            }

            if (key == Missing) return new ValueTask<T?>(default(T));
            return new ValueTask<T?>((T)(object)("typed:" + key));
        }

        public ValueTask<byte[]?> GetBytesAsync(string key, CancellationToken cancellationToken = default)
        {
            lock (_gate)
            {
                ByteReads.Add(key);
                Tokens.Add(cancellationToken);
            }

            if (key == Missing) return new ValueTask<byte[]?>((byte[]?)null);
            return new ValueTask<byte[]?>(System.Text.Encoding.UTF8.GetBytes("bytes:" + key));
        }

        public ValueTask SetAsync<T>(string key, T value, TimeSpan? expiry = null, CancellationToken cancellationToken = default) => default;
        public ValueTask SetBytesAsync(string key, ReadOnlyMemory<byte> value, TimeSpan? expiry = null, CancellationToken cancellationToken = default) => default;
        public ValueTask<bool> RemoveAsync(string key, CancellationToken cancellationToken = default) => new(false);
        public void EvictLocal(string key) { }
        public void EvictAllLocal() { }
        public bool TryGetLocal<T>(string key, out T? value) { value = default; return false; }
        public RedisNearCacheStatistics Statistics { get; } = new();
        public Task Ready => Task.CompletedTask;
        public bool IsCoherent => true;
        public Task WaitForCoherenceAsync(CancellationToken cancellationToken = default) => Task.CompletedTask;
        public ValueTask DisposeAsync() => default;
    }

    /// <summary>A wrapper that overrides the single-key typed read only and delegates the rest.</summary>
    private sealed class DecoratingCache(IRedisNearCache inner) : IRedisNearCache
    {
        public async ValueTask<T?> GetAsync<T>(string key, CancellationToken cancellationToken = default)
        {
            var value = await inner.GetAsync<T>(key, cancellationToken);
            return (T)(object)$"decorated({value})";
        }

        public ValueTask<byte[]?> GetBytesAsync(string key, CancellationToken cancellationToken = default) => inner.GetBytesAsync(key, cancellationToken);
        public ValueTask SetAsync<T>(string key, T value, TimeSpan? expiry = null, CancellationToken cancellationToken = default) => inner.SetAsync(key, value, expiry, cancellationToken);
        public ValueTask SetBytesAsync(string key, ReadOnlyMemory<byte> value, TimeSpan? expiry = null, CancellationToken cancellationToken = default) => inner.SetBytesAsync(key, value, expiry, cancellationToken);
        public ValueTask<bool> RemoveAsync(string key, CancellationToken cancellationToken = default) => inner.RemoveAsync(key, cancellationToken);
        public void EvictLocal(string key) => inner.EvictLocal(key);
        public void EvictAllLocal() => inner.EvictAllLocal();
        public bool TryGetLocal<T>(string key, out T? value) => inner.TryGetLocal(key, out value);
        public RedisNearCacheStatistics Statistics => inner.Statistics;
        public Task Ready => inner.Ready;
        public bool IsCoherent => inner.IsCoherent;
        public Task WaitForCoherenceAsync(CancellationToken cancellationToken = default) => inner.WaitForCoherenceAsync(cancellationToken);
        public ValueTask DisposeAsync() => inner.DisposeAsync();
    }
}
