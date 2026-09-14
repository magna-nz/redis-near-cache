using StackExchange.Redis;

namespace RedisNearCache;

/// <summary>Configuration for <see cref="IRedisNearCache"/>.</summary>
public sealed class RedisNearCacheOptions
{
    /// <summary>
    /// Connection settings for the Redis deployment. RedisNearCache CLONES these and forces
    /// <c>Protocol=Resp2</c>, <c>AllowAdmin=true</c> and its own <c>ClientName</c>; the caller's own multiplexer
    /// is never touched. Either this or <see cref="ConnectionString"/> must be set.
    /// </summary>
    public ConfigurationOptions? Configuration { get; set; }

    /// <summary>Alternative to <see cref="Configuration"/>; parsed with <see cref="ConfigurationOptions.Parse(string)"/>.</summary>
    public string? ConnectionString { get; set; }

    /// <summary>
    /// Key prefixes that RedisNearCache will cache locally. Reads of keys outside these prefixes still go to Redis
    /// through RedisNearCache but are not stored in L1. This does not change server-side tracking: every key read
    /// through RedisNearCache is still tracked, so writes to it still push invalidations. To avoid those, read the
    /// key through your own multiplexer instead. Empty (default) means every key read through RedisNearCache is cached.
    /// </summary>
    public IList<string> KeyPrefixes { get; } = new List<string>();

    /// <summary>Maximum number of entries held in L1. Least-recently-used entries are evicted beyond this.</summary>
    public long L1SizeLimit { get; set; } = 10_000;

    /// <summary>
    /// Safety net: an L1 entry is dropped after this age even if no invalidation arrived.
    /// Protects against a missed invalidation. Set to <see cref="Timeout.InfiniteTimeSpan"/> to disable.
    /// </summary>
    public TimeSpan L1MaxAge { get; set; } = TimeSpan.FromMinutes(5);

    /// <summary>Serializer for values. Defaults to System.Text.Json; <c>string</c> and <c>byte[]</c> pass through.</summary>
    public IRedisNearCacheSerializer Serializer { get; set; } = JsonRedisNearCacheSerializer.Instance;

    /// <summary>Prefix for the Redis client name RedisNearCache sets on its connections. A unique suffix is appended.</summary>
    public string ClientNamePrefix { get; set; } = "rnc";

    /// <summary>Hooks used by the integration tests to inject timing. Not for production use.</summary>
    internal RedisNearCacheTestHooks TestHooks { get; } = new();
}
