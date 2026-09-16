namespace RedisNearCache.Tests;

/// <summary>A serializer that fails the test if anything reaches it: proves a path uses the raw-bytes members only.</summary>
internal sealed class RefusingSerializer : IRedisNearCacheSerializer
{
    public byte[] Serialize<T>(T value) => throw new InvalidOperationException($"the serializer was asked to serialize a {typeof(T).Name}");

    public T? Deserialize<T>(ReadOnlyMemory<byte> data) => throw new InvalidOperationException($"the serializer was asked to deserialize a {typeof(T).Name}");
}

internal static class RawBytes
{
    /// <summary>Bytes that are not valid UTF-8 or JSON, so any re-encoding on the way shows up as a mismatch.</summary>
    public static byte[] Payload() => [0x00, 0xFF, 0xFE, 0x80, 0x7B, 0x22, 0x0A, 0xC3];

    /// <summary>
    /// Reads <paramref name="key"/> with <see cref="IRedisNearCache.GetBytesAsync"/> until a read returns
    /// <paramref name="expected"/> from L1 (the hit counter moves). In Broadcast mode the echo of the cache's own
    /// write can evict the first cached copy, so one read-then-check is not enough.
    /// </summary>
    public static async Task<bool> ReadUntilCachedAsync(IRedisNearCache cache, string key, byte[] expected, TimeSpan? timeout = null)
    {
        var deadline = DateTime.UtcNow + (timeout ?? TimeSpan.FromSeconds(10));
        while (DateTime.UtcNow < deadline)
        {
            var hits = cache.Statistics.Hits;
            var value = await cache.GetBytesAsync(key);
            if (value is not null && value.AsSpan().SequenceEqual(expected) && cache.Statistics.Hits > hits)
            {
                return true;
            }

            await Task.Delay(20);
        }

        return false;
    }
}
