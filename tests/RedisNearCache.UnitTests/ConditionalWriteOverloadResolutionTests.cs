using StackExchange.Redis;
using Xunit;

namespace RedisNearCache.UnitTests;

/// <summary>
/// This file exists to fail the BUILD, not just a test, if an overload ambiguity is ever introduced between
/// <c>SetAsync&lt;T&gt;(key, value, expiry, cancellationToken)</c> / <c>SetBytesAsync(key, value, expiry,
/// cancellationToken)</c> and the new conditional-write overloads that add a <see cref="When"/> parameter. Every
/// call shape below is exactly one that existing callers use today; if a future change to
/// <see cref="IRedisNearCache"/> (e.g. giving <c>when</c> a default value, or moving it after <c>expiry</c>) makes
/// any of them ambiguous, this file stops compiling before any assertion ever runs. The assertions on top of that
/// confirm each shape still binds to the overload it always bound to (old shapes -&gt; the old members; new shapes
/// -&gt; the new ones), via a fake that records which member was invoked.
/// </summary>
public class ConditionalWriteOverloadResolutionTests
{
    private const string Key = "k";

    [Fact]
    public async Task ExistingSetAsyncCallShapesStillBindToTheOldOverload()
    {
        var fake = new RecordingCache();
        IRedisNearCache cache = fake;
        var ct = CancellationToken.None;

        await cache.SetAsync(Key, "v1");
        await cache.SetAsync(Key, "v2", TimeSpan.FromSeconds(1));
        await cache.SetAsync(Key, "v3", TimeSpan.FromSeconds(1), default);
        await cache.SetAsync(Key, "v4", TimeSpan.FromSeconds(1), CancellationToken.None);
        await cache.SetAsync(Key, "v5", null, ct);
        await cache.SetAsync(Key, "v6", cancellationToken: ct);

        Assert.Equal(["old:v1", "old:v2", "old:v3", "old:v4", "old:v5", "old:v6"], fake.Calls);
    }

    [Fact]
    public async Task ExistingSetBytesAsyncCallShapesStillBindToTheOldOverload()
    {
        var fake = new RecordingCache();
        IRedisNearCache cache = fake;
        var ct = CancellationToken.None;
        byte[] payload = [1];

        await cache.SetBytesAsync(Key, payload);
        await cache.SetBytesAsync(Key, payload, TimeSpan.FromSeconds(1));
        await cache.SetBytesAsync(Key, payload, TimeSpan.FromSeconds(1), default);
        await cache.SetBytesAsync(Key, payload, TimeSpan.FromSeconds(1), CancellationToken.None);
        await cache.SetBytesAsync(Key, payload, null, ct);
        await cache.SetBytesAsync(Key, payload, cancellationToken: ct);

        Assert.Equal(["oldBytes", "oldBytes", "oldBytes", "oldBytes", "oldBytes", "oldBytes"], fake.Calls);
    }

    [Fact]
    public async Task NewSetAsyncCallShapesBindToTheConditionalOverload()
    {
        var fake = new RecordingCache();
        IRedisNearCache cache = fake;
        var ct = CancellationToken.None;

        await cache.SetAsync(Key, "v1", When.NotExists);
        await cache.SetAsync(Key, "v2", When.Exists, TimeSpan.FromSeconds(1));
        await cache.SetAsync(Key, "v3", When.Always, keepTtl: true);
        await cache.SetAsync(Key, "v4", When.NotExists, TimeSpan.FromSeconds(2), false, ct);

        Assert.Equal(
        [
            "new:v1:NotExists:.:False",
            "new:v2:Exists:00:00:01:False",
            "new:v3:Always:.:True",
            "new:v4:NotExists:00:00:02:False",
        ], fake.Calls);
    }

    [Fact]
    public async Task NewSetBytesAsyncCallShapesBindToTheConditionalOverload()
    {
        var fake = new RecordingCache();
        IRedisNearCache cache = fake;
        var ct = CancellationToken.None;
        byte[] payload = [1];

        await cache.SetBytesAsync(Key, payload, When.NotExists);
        await cache.SetBytesAsync(Key, payload, When.Exists, TimeSpan.FromSeconds(1));
        await cache.SetBytesAsync(Key, payload, When.Always, keepTtl: true);
        await cache.SetBytesAsync(Key, payload, When.NotExists, TimeSpan.FromSeconds(2), false, ct);

        Assert.Equal(
        [
            "newBytes:NotExists:.:False",
            "newBytes:Exists:00:00:01:False",
            "newBytes:Always:.:True",
            "newBytes:NotExists:00:00:02:False",
        ], fake.Calls);
    }

    /// <summary>Records which member of <see cref="IRedisNearCache"/> each call reached, and with what arguments.</summary>
    private sealed class RecordingCache : IRedisNearCache
    {
        public readonly List<string> Calls = new();

        public ValueTask<T?> GetAsync<T>(string key, CancellationToken cancellationToken = default) => default;
        public ValueTask<byte[]?> GetBytesAsync(string key, CancellationToken cancellationToken = default) => default;

        public ValueTask SetAsync<T>(string key, T value, TimeSpan? expiry = null, CancellationToken cancellationToken = default)
        {
            Calls.Add($"old:{value}");
            return default;
        }

        public ValueTask SetBytesAsync(string key, ReadOnlyMemory<byte> value, TimeSpan? expiry = null, CancellationToken cancellationToken = default)
        {
            Calls.Add("oldBytes");
            return default;
        }

        public ValueTask<bool> SetAsync<T>(string key, T value, When when, TimeSpan? expiry = null, bool keepTtl = false, CancellationToken cancellationToken = default)
        {
            Calls.Add($"new:{value}:{when}:{(expiry is null ? "." : expiry.Value.ToString())}:{keepTtl}");
            return new(true);
        }

        public ValueTask<bool> SetBytesAsync(string key, ReadOnlyMemory<byte> value, When when, TimeSpan? expiry = null, bool keepTtl = false, CancellationToken cancellationToken = default)
        {
            Calls.Add($"newBytes:{when}:{(expiry is null ? "." : expiry.Value.ToString())}:{keepTtl}");
            return new(true);
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
