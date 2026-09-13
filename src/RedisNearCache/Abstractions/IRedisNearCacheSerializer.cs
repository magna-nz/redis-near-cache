using System.Text;
using System.Text.Json;

namespace RedisNearCache;

/// <summary>Converts values to and from the bytes stored in Redis.</summary>
public interface IRedisNearCacheSerializer
{
    byte[] Serialize<T>(T value);
    T? Deserialize<T>(ReadOnlyMemory<byte> data);
}

/// <summary>Default serializer: System.Text.Json, with <c>string</c> and <c>byte[]</c> passed through untouched.</summary>
public sealed class JsonRedisNearCacheSerializer : IRedisNearCacheSerializer
{
    public static JsonRedisNearCacheSerializer Instance { get; } = new(JsonSerializerOptions.Default);

    private readonly JsonSerializerOptions _options;

    public JsonRedisNearCacheSerializer(JsonSerializerOptions options) => _options = options;

    public byte[] Serialize<T>(T value) => value switch
    {
        string s => Encoding.UTF8.GetBytes(s),
        byte[] b => b,
        _ => JsonSerializer.SerializeToUtf8Bytes(value, _options),
    };

    public T? Deserialize<T>(ReadOnlyMemory<byte> data)
    {
        if (typeof(T) == typeof(string)) return (T)(object)Encoding.UTF8.GetString(data.Span);
        if (typeof(T) == typeof(byte[])) return (T)(object)data.ToArray();
        return JsonSerializer.Deserialize<T>(data.Span, _options);
    }
}
