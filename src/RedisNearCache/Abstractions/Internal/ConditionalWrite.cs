using StackExchange.Redis;

namespace RedisNearCache.Internal;

/// <summary>Argument rules shared by the conditional write members of <see cref="IRedisNearCache"/> and the cache itself.</summary>
internal static class ConditionalWrite
{
    /// <summary>Redis rejects <c>SET ... EX n KEEPTTL</c>; fail before the round trip, and the same way for every implementation.</summary>
    public static void ThrowIfInvalid(TimeSpan? expiry, bool keepTtl)
    {
        if (keepTtl && expiry is not null)
        {
            throw new ArgumentException("keepTtl keeps the key's existing TTL, so it cannot be combined with an expiry.", nameof(keepTtl));
        }
    }

    /// <summary>The interface's default implementation can only fall back to the unconditional write it already had.</summary>
    public static void ThrowIfNotExpressible(When when, bool keepTtl)
    {
        if (when != When.Always || keepTtl)
        {
            throw new NotSupportedException("This IRedisNearCache implementation does not support conditional writes or keepTtl.");
        }
    }
}
