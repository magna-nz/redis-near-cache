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
    /// through RedisNearCache but are neither stored in L1 nor tracked by the server: the connection is armed in
    /// <c>OPTOUT</c> mode and such reads are sent with <c>CLIENT CACHING NO</c> (inside a MULTI/EXEC so the two are
    /// adjacent on the wire), so writes to them push no invalidation. While the cache is in pass-through, or if the
    /// transaction could not run, such a read falls back to a plain GET and is tracked like any other. Empty
    /// (default) means every key read through RedisNearCache is cached and tracked. In <see cref="TrackingMode.Broadcast"/>
    /// these are also the <c>BCAST PREFIX</c> arguments. Read once when the cache is created; later changes are ignored.
    /// When <see cref="KeyNamespace"/> is set these are relative to it, like every other key given to the cache:
    /// <c>"user:"</c> under the namespace <c>"app1:"</c> means the Redis keys starting <c>app1:user:</c>.
    /// </summary>
    public IList<string> KeyPrefixes { get; } = new List<string>();

    /// <summary>
    /// A prefix put in front of every key given to the cache, so several applications, tenants or cache instances can
    /// share one Redis database without their keys meeting: with <c>"app1:"</c>, <c>GetAsync("user:42")</c> reads the
    /// Redis key <c>app1:user:42</c>. Callers keep using their own keys everywhere, including in the result of
    /// <see cref="IRedisNearCache.GetManyAsync{T}"/>; anything that writes to Redis directly must use the full key for
    /// the write to invalidate the local copy. <see cref="KeyPrefixes"/> are relative to it, and in
    /// <see cref="TrackingMode.Broadcast"/> a namespace with no <see cref="KeyPrefixes"/> arms the server with the
    /// namespace itself instead of the whole keyspace. When adopting it, stop passing full keys: a key that already
    /// begins with the namespace gets it a second time (logged once as a warning). Log messages name the full key, as
    /// Redis sees it. On a cluster keep <c>{</c>...<c>}</c> out of the namespace: it would be a hash tag, and every key
    /// would land in one slot. <c>null</c> or empty (the default)
    /// changes nothing: keys go to Redis exactly as given. Read once when the cache is created.
    /// </summary>
    public string? KeyNamespace { get; set; }

    /// <summary>
    /// How invalidations reach this instance. <see cref="TrackingMode.Redirect"/> (default) for Redis and Valkey
    /// servers reached directly; <see cref="TrackingMode.Broadcast"/> for Redis Enterprise-based services
    /// (Azure Managed Redis, Redis Cloud, Redis Software), which combine with <see cref="KeyPrefixes"/>.
    /// </summary>
    public TrackingMode TrackingMode { get; set; } = TrackingMode.Redirect;

    /// <summary>
    /// Maximum number of entries held in L1. Least-recently-used entries are evicted beyond this.
    /// Ignored when <see cref="L1SizeLimitBytes"/> is set.
    /// </summary>
    public long L1SizeLimit { get; set; } = 10_000;

    /// <summary>
    /// Bounds L1 by the total size in bytes of the cached values rather than by the number of entries; when set,
    /// <see cref="L1SizeLimit"/> is ignored. Each entry costs the length of its value (a zero-length value costs 1),
    /// and a single value larger than the limit is never cached at all - the read still returns it to the caller,
    /// it just comes from Redis every time. Only the values are counted: the keys, and MemoryCache's own per-entry
    /// overhead, are not, so the process holds somewhat more than this. <c>null</c> (the default) keeps the
    /// entry-count behaviour of <see cref="L1SizeLimit"/>.
    /// </summary>
    public long? L1SizeLimitBytes { get; set; }

    /// <summary>
    /// Safety net: an L1 entry is dropped after this age even if no invalidation arrived.
    /// Protects against a missed invalidation. Set to <see cref="Timeout.InfiniteTimeSpan"/> to disable.
    /// </summary>
    public TimeSpan L1MaxAge { get; set; } = TimeSpan.FromMinutes(5);

    /// <summary>
    /// On every miss, read the key's remaining TTL (<c>PTTL</c>, pipelined with the <c>GET</c>: one extra command,
    /// no extra round trip) and never keep the L1 entry past it. Redis only pushes an expiry invalidation when its
    /// active-expiry cycle actually deletes the key, which can lag the TTL deadline by minutes on a large
    /// keyspace; with this on, a value is never served locally after its TTL has elapsed. Off means the entry
    /// lives until an invalidation arrives or <see cref="L1MaxAge"/> drops it.
    /// </summary>
    public bool RespectServerTtl { get; set; } = true;

    /// <summary>Serializer for values. Defaults to System.Text.Json; <c>string</c> and <c>byte[]</c> pass through.</summary>
    public IRedisNearCacheSerializer Serializer { get; set; } = JsonRedisNearCacheSerializer.Instance;

    /// <summary>Prefix for the Redis client name RedisNearCache sets on its connections. A unique suffix is appended.</summary>
    public string ClientNamePrefix { get; set; } = "rnc";

    /// <summary>The name this instance was registered under with <c>AddKeyedRedisNearCache</c>; null for the default one.</summary>
    internal string? InstanceName { get; set; }

    /// <summary>
    /// <see cref="KeyPrefixes"/> as Redis sees them: with <see cref="KeyNamespace"/> in front, or the namespace alone
    /// when there are none. The cache's own filter and the Broadcast tracker's <c>BCAST PREFIX</c> list both come from
    /// here, because the two must agree. Without a namespace this is <see cref="KeyPrefixes"/> unchanged.
    /// </summary>
    internal string[] EffectiveKeyPrefixes()
    {
        var keyNamespace = KeyNamespace;
        if (string.IsNullOrEmpty(keyNamespace)) return KeyPrefixes.ToArray();
        return KeyPrefixes.Count == 0 ? [keyNamespace] : KeyPrefixes.Select(prefix => keyNamespace + prefix).ToArray();
    }

    /// <summary>Hooks used by the integration tests to inject timing. Not for production use.</summary>
    internal RedisNearCacheTestHooks TestHooks { get; } = new();
}
