using StackExchange.Redis;
using Xunit;

namespace RedisNearCache.UnitTests;

/// <summary>
/// The default interface method bodies of <see cref="IRedisNearCache.SetAsync{T}(string, T, When, TimeSpan?, bool, CancellationToken)"/>
/// and <see cref="IRedisNearCache.SetBytesAsync(string, ReadOnlyMemory{byte}, When, TimeSpan?, bool, CancellationToken)"/>,
/// exercised through a fake that implements only the members that predate them: an implementation that has not been
/// updated must still compile and still behave exactly as documented.
/// </summary>
public class ConditionalWriteDefaultImplementationTests
{
    private const string Key = "k";

    [Fact]
    public async Task UnconditionalAlwaysWithoutKeepTtlDelegatesToTheOldMemberAndReturnsTrue()
    {
        var fake = new LegacyCache();
        IRedisNearCache cache = fake;

        var result = await cache.SetAsync(Key, "v1", When.Always, TimeSpan.FromSeconds(5));

        Assert.True(result);
        Assert.NotNull(fake.LastSet);
        Assert.Equal(Key, fake.LastSet!.Value.Key);
        Assert.Equal("v1", fake.LastSet.Value.Value);
        Assert.Equal(TimeSpan.FromSeconds(5), fake.LastSet.Value.Expiry);
    }

    [Fact]
    public async Task UnconditionalAlwaysWithoutKeepTtlDelegatesToTheOldBytesMemberAndReturnsTrue()
    {
        var fake = new LegacyCache();
        IRedisNearCache cache = fake;
        byte[] payload = [1, 2, 3];

        var result = await cache.SetBytesAsync(Key, payload, When.Always, TimeSpan.FromSeconds(5));

        Assert.True(result);
        Assert.NotNull(fake.LastSetBytes);
        Assert.Equal(Key, fake.LastSetBytes!.Value.Key);
        Assert.Equal(payload, fake.LastSetBytes.Value.Value);
        Assert.Equal(TimeSpan.FromSeconds(5), fake.LastSetBytes.Value.Expiry);
    }

    [Theory]
    [InlineData(When.NotExists)]
    [InlineData(When.Exists)]
    public async Task AConditionThrowsNotSupportedAndNeverReachesTheOldMember(When when)
    {
        var fake = new LegacyCache();
        IRedisNearCache cache = fake;

        await Assert.ThrowsAsync<NotSupportedException>(() => cache.SetAsync(Key, "v1", when).AsTask());

        Assert.Null(fake.LastSet);
    }

    [Theory]
    [InlineData(When.NotExists)]
    [InlineData(When.Exists)]
    public async Task AConditionThrowsNotSupportedAndNeverReachesTheOldBytesMember(When when)
    {
        var fake = new LegacyCache();
        IRedisNearCache cache = fake;

        await Assert.ThrowsAsync<NotSupportedException>(() => cache.SetBytesAsync(Key, new byte[] { 1 }, when).AsTask());

        Assert.Null(fake.LastSetBytes);
    }

    [Fact]
    public async Task KeepTtlAloneThrowsNotSupportedAndNeverReachesTheOldMember()
    {
        var fake = new LegacyCache();
        IRedisNearCache cache = fake;

        await Assert.ThrowsAsync<NotSupportedException>(() => cache.SetAsync(Key, "v1", When.Always, keepTtl: true).AsTask());

        Assert.Null(fake.LastSet);
    }

    [Fact]
    public async Task KeepTtlAloneThrowsNotSupportedAndNeverReachesTheOldBytesMember()
    {
        var fake = new LegacyCache();
        IRedisNearCache cache = fake;

        await Assert.ThrowsAsync<NotSupportedException>(() => cache.SetBytesAsync(Key, new byte[] { 1 }, When.Always, keepTtl: true).AsTask());

        Assert.Null(fake.LastSetBytes);
    }

    [Theory]
    [InlineData(When.Always)]
    [InlineData(When.NotExists)]
    [InlineData(When.Exists)]
    public async Task KeepTtlWithExpiryThrowsArgumentExceptionForEveryWhen(When when)
    {
        var fake = new LegacyCache();
        IRedisNearCache cache = fake;

        var ex = await Assert.ThrowsAsync<ArgumentException>(
            () => cache.SetAsync(Key, "v1", when, TimeSpan.FromSeconds(1), keepTtl: true).AsTask());

        Assert.Equal("keepTtl", ex.ParamName);
        Assert.Null(fake.LastSet);
    }

    [Theory]
    [InlineData(When.Always)]
    [InlineData(When.NotExists)]
    [InlineData(When.Exists)]
    public async Task KeepTtlWithExpiryThrowsArgumentExceptionForEveryWhenOnTheBytesMember(When when)
    {
        var fake = new LegacyCache();
        IRedisNearCache cache = fake;

        var ex = await Assert.ThrowsAsync<ArgumentException>(
            () => cache.SetBytesAsync(Key, new byte[] { 1 }, when, TimeSpan.FromSeconds(1), keepTtl: true).AsTask());

        Assert.Equal("keepTtl", ex.ParamName);
        Assert.Null(fake.LastSetBytes);
    }

    /// <summary>An <see cref="IRedisNearCache"/> that predates the conditional-write members: it implements
    /// only what existed before them, and relies entirely on the interface's default implementation for the rest.</summary>
    private sealed class LegacyCache : IRedisNearCache
    {
        public (string Key, string Value, TimeSpan? Expiry, CancellationToken CancellationToken)? LastSet;
        public (string Key, byte[] Value, TimeSpan? Expiry, CancellationToken CancellationToken)? LastSetBytes;

        public ValueTask<T?> GetAsync<T>(string key, CancellationToken cancellationToken = default) => default;
        public ValueTask<byte[]?> GetBytesAsync(string key, CancellationToken cancellationToken = default) => default;

        public ValueTask SetAsync<T>(string key, T value, TimeSpan? expiry = null, CancellationToken cancellationToken = default)
        {
            LastSet = (key, (string)(object)value!, expiry, cancellationToken);
            return default;
        }

        public ValueTask SetBytesAsync(string key, ReadOnlyMemory<byte> value, TimeSpan? expiry = null, CancellationToken cancellationToken = default)
        {
            LastSetBytes = (key, value.ToArray(), expiry, cancellationToken);
            return default;
        }

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
}
