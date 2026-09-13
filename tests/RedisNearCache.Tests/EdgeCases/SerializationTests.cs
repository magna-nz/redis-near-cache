using System.Text;
using Xunit;

namespace RedisNearCache.Tests.EdgeCases;

/// <summary>
/// <see cref="JsonRedisNearCacheSerializer"/> passes <c>byte[]</c> and <c>string</c> through untouched
/// (src/RedisNearCache/Abstractions/IRedisNearCacheSerializer.cs); everything else is JSON. A custom
/// <see cref="IRedisNearCacheSerializer"/> must be used end to end: what actually lands in Redis is whatever
/// the serializer produced, not the default.
/// </summary>
public class SerializationTests : IClassFixture<StandaloneCacheFixture>
{
    private readonly StandaloneCacheFixture _fx;

    public SerializationTests(StandaloneCacheFixture fx) => _fx = fx;

    [Fact]
    public async Task ByteArrayAndStringPassThrough()
    {
        var bytesKey = TestHelpers.Key("bytes-passthrough");
        var bytes = new byte[256];
        for (var i = 0; i < bytes.Length; i++) bytes[i] = (byte)i;

        await _fx.Cache.SetAsync(bytesKey, bytes);
        var roundTripped = await _fx.Cache.GetAsync<byte[]>(bytesKey);
        Assert.NotNull(roundTripped);
        Assert.Equal(bytes, roundTripped);

        var stringKey = TestHelpers.Key("string-passthrough");
        const string literal = "plain literal, no json quoting";
        await _fx.Cache.SetAsync(stringKey, literal);
        Assert.Equal(literal, RedisCli.Standalone("GET", stringKey));
    }

    [Fact]
    public async Task CustomSerializerIsUsed()
    {
        var handle = await EdgeCaseSupport.BuildAsync(StandaloneCacheFixture.ConnectionString, o => o.Serializer = new MarkerSerializer());
        try
        {
            var key = TestHelpers.Key("custom-serializer");
            await handle.Cache.SetAsync(key, "hello");

            var raw = RedisCli.Standalone("GET", key);
            Assert.StartsWith(MarkerSerializer.Marker, raw, StringComparison.Ordinal);

            var decoded = await handle.Cache.GetAsync<string>(key);
            Assert.Equal("hello", decoded);
        }
        finally
        {
            await handle.DisposeAsync();
        }
    }

    /// <summary>Prefixes every serialized string with a fixed marker, so a redis-cli GET proves the custom serializer ran.</summary>
    private sealed class MarkerSerializer : IRedisNearCacheSerializer
    {
        public const string Marker = "MARK:";

        public byte[] Serialize<T>(T value) => value switch
        {
            string s => Encoding.UTF8.GetBytes(Marker + s),
            _ => throw new NotSupportedException($"{nameof(MarkerSerializer)} only supports string in this test."),
        };

        public T? Deserialize<T>(ReadOnlyMemory<byte> data)
        {
            var text = Encoding.UTF8.GetString(data.Span);
            if (!text.StartsWith(Marker, StringComparison.Ordinal))
                throw new InvalidOperationException("expected the marker prefix written by Serialize.");
            var stripped = text[Marker.Length..];
            if (typeof(T) == typeof(string)) return (T)(object)stripped;
            throw new NotSupportedException($"{nameof(MarkerSerializer)} only supports string in this test.");
        }
    }
}
