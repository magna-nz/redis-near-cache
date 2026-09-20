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
/// <remarks>
/// Both directions dispatch on the static type, never on the runtime value, so that what
/// <see cref="Serialize{T}"/> writes is always what <see cref="Deserialize{T}"/> for the same <c>T</c> reads back.
/// Dispatching <c>Serialize</c> on the value instead made the two disagree twice over: a declaration pattern never
/// matches <c>null</c>, so <c>Serialize&lt;string&gt;(null)</c> fell through to JSON and wrote the four bytes
/// <c>null</c>, which <c>Deserialize&lt;string&gt;</c> then handed back as the four-character string "null"; and
/// <c>Serialize&lt;object&gt;("abc")</c> wrote raw bytes that <c>Deserialize&lt;object&gt;</c> could only throw on.
/// </remarks>
public sealed class JsonRedisNearCacheSerializer : IRedisNearCacheSerializer
{
    /// <summary>Shared instance using <see cref="JsonSerializerOptions.Default"/>.</summary>
    public static JsonRedisNearCacheSerializer Instance { get; } = new(JsonSerializerOptions.Default);

    private readonly JsonSerializerOptions _options;

    /// <summary>Creates a serializer with custom <see cref="JsonSerializerOptions"/>.</summary>
    public JsonRedisNearCacheSerializer(JsonSerializerOptions options) => _options = options;

    /// <inheritdoc />
    /// <exception cref="ArgumentNullException">
    /// <paramref name="value"/> is null and <typeparamref name="T"/> is <c>string</c> or <c>byte[]</c>, which pass
    /// through untouched and so have no way to represent null: Redis holds bytes or holds nothing. Store nothing, or
    /// remove the key, to mean absent.
    /// </exception>
    public byte[] Serialize<T>(T value)
    {
        if (typeof(T) == typeof(string))
        {
            ArgumentNullException.ThrowIfNull(value);
            return Encoding.UTF8.GetBytes((string)(object)value);
        }

        if (typeof(T) == typeof(byte[]))
        {
            ArgumentNullException.ThrowIfNull(value);
            return (byte[])(object)value;
        }

        return JsonSerializer.SerializeToUtf8Bytes(value, _options);
    }

    /// <inheritdoc />
    public T? Deserialize<T>(ReadOnlyMemory<byte> data)
    {
        if (typeof(T) == typeof(string)) return (T)(object)Encoding.UTF8.GetString(data.Span);
        if (typeof(T) == typeof(byte[])) return (T)(object)data.ToArray();
        return JsonSerializer.Deserialize<T>(data.Span, _options);
    }
}
