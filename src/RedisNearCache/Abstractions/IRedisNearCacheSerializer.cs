using System.Text;
using System.Text.Json;

namespace RedisNearCache;

/// <summary>Converts values to and from the bytes stored in Redis.</summary>
public interface IRedisNearCacheSerializer
{
    /// <summary>Serializes <paramref name="value"/> to the bytes stored in Redis.</summary>
    byte[] Serialize<T>(T value);

    /// <summary>Deserializes bytes read from Redis (or held in L1) back to <typeparamref name="T"/>.</summary>
    T? Deserialize<T>(ReadOnlyMemory<byte> data);
}

/// <summary>Default serializer: System.Text.Json, with <c>string</c> and <c>byte[]</c> passed through untouched.</summary>
public sealed class JsonRedisNearCacheSerializer : IRedisNearCacheSerializer
{
    /// <summary>Shared instance using <see cref="JsonSerializerOptions.Default"/>.</summary>
    public static JsonRedisNearCacheSerializer Instance { get; } = new(JsonSerializerOptions.Default);

    private readonly JsonSerializerOptions _options;

    /// <summary>Creates a serializer with custom <see cref="JsonSerializerOptions"/>.</summary>
    public JsonRedisNearCacheSerializer(JsonSerializerOptions options) => _options = options;

    /// <inheritdoc />
    public byte[] Serialize<T>(T value) => value switch
    {
        string s => Encoding.UTF8.GetBytes(s),
        byte[] b => b,
        _ => JsonSerializer.SerializeToUtf8Bytes(value, _options),
    };

    /// <inheritdoc />
    public T? Deserialize<T>(ReadOnlyMemory<byte> data)
    {
        if (typeof(T) == typeof(string)) return (T)(object)Encoding.UTF8.GetString(data.Span);
        if (typeof(T) == typeof(byte[])) return (T)(object)data.ToArray();
        return JsonSerializer.Deserialize<T>(data.Span, _options);
    }
}
