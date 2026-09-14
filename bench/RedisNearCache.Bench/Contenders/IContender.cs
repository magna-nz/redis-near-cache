namespace RedisNearCache.Bench.Contenders;

/// <summary>The caching strategies the benchmarks compare. Names are used verbatim on the command line (case-insensitive) and in result files.</summary>
public enum ContenderKind
{
    /// <summary>Plain StackExchange.Redis <c>GET</c>/<c>SET</c>. No local tier. The floor.</summary>
    Plain,

    /// <summary><c>IMemoryCache</c> with an absolute TTL in front of Redis: the common hand-rolled near cache. Nothing ever invalidates it.</summary>
    MemoryCacheTtl,

    /// <summary><c>Microsoft.Extensions.Caching.Hybrid.HybridCache</c> with its default in-process L1 and a Redis <c>IDistributedCache</c> as L2.</summary>
    HybridCache,

    /// <summary>FusionCache with an in-process L1, a Redis L2, and the Redis backplane (cross-instance invalidation for writes made through FusionCache).</summary>
    FusionCache,

    /// <summary><c>IRedisNearCache</c> used directly.</summary>
    NearCache,

    /// <summary><c>HybridCache</c> over <c>AddRedisNearCacheHybridCache</c> (HybridCache's own L1 disabled; RedisNearCache's tracked L1 behind it).</summary>
    NearCacheHybridCache,
}

/// <summary>Where writes during a load run come from.</summary>
public enum WriteMode
{
    /// <summary>
    /// Another client (another service, another language) writes the source-of-truth key straight to Redis with a plain
    /// <c>SET</c>. No contender's API is involved, so only server-side tracking can notice.
    /// </summary>
    Foreign,

    /// <summary>
    /// Writes go through <see cref="IContender.SetAsync{T}"/> on a dedicated writer instance of the same contender kind, so
    /// whatever cross-instance mechanism the library offers (a backplane, tracking) gets its chance.
    /// </summary>
    Api,
}

/// <summary>Configuration shared by every contender. Identical values for every kind in a run, so the comparison is fair.</summary>
public sealed record ContenderSettings
{
    /// <summary>StackExchange.Redis configuration string, e.g. <c>localhost:6420</c> or <c>localhost:6390,ssl=true,...</c>.</summary>
    public required string Endpoint { get; init; }

    /// <summary>
    /// PEM CA certificate to trust when <see cref="Endpoint"/> has <c>ssl=true</c> (the repo's TLS container uses a private CA,
    /// <c>certs/ca.crt</c>). Every Redis connection any kind opens must honour it.
    /// </summary>
    public string? TlsCaCertificatePath { get; init; }

    /// <summary>
    /// Local-tier lifetime for kinds that rely on expiry to bound staleness (<see cref="ContenderKind.MemoryCacheTtl"/>,
    /// <see cref="ContenderKind.HybridCache"/>, <see cref="ContenderKind.FusionCache"/>), and the L2 entry lifetime for kinds
    /// that have one. Ignored by <see cref="ContenderKind.Plain"/> and <see cref="ContenderKind.NearCache"/>.
    /// </summary>
    public TimeSpan Ttl { get; init; } = TimeSpan.FromSeconds(10);

    /// <summary>Maximum local entries, applied to every kind that has a local tier (RedisNearCache <c>L1SizeLimit</c>, <c>MemoryCacheOptions.SizeLimit</c> with size 1 per entry, etc.).</summary>
    public long LocalSizeLimit { get; init; } = 20_000;

    /// <summary>Distinguishes instances in one process (client names, L2 key prefixes where a library needs one). Unique per instance.</summary>
    public string InstanceName { get; init; } = "bench";
}

/// <summary>
/// One application instance using one caching strategy.
/// <para>
/// <b>Source of truth.</b> Every kind reads and writes the same Redis key, <c>key</c>, holding the raw value: a
/// <see cref="string"/> is stored as its UTF-8 bytes, anything else as System.Text.Json UTF-8. Foreign writers write that
/// key directly. A kind with an L2 of its own (HybridCache, FusionCache) keeps its L2 entries under its own prefix and
/// loads from the source-of-truth key in its factory; it must never store its own payload format at <c>key</c>.
/// </para>
/// <para>
/// <b>Locality proxy.</b> The load test classifies a read as local when the returned <see cref="ValueTask{TResult}"/> is
/// already completed (<see cref="ValueTask{TResult}.IsCompletedSuccessfully"/>). Implementations must preserve that:
/// return the library's own <see cref="ValueTask{TResult}"/> where possible, and never add an <c>await</c>, a
/// <c>Task.Yield</c>, or a <c>Task</c> wrapper on a path that did not touch the network. A kind whose library cannot
/// complete a local hit synchronously reports that in <see cref="LocalHitsCompleteSynchronously"/>.
/// </para>
/// </summary>
public interface IContender : IAsyncDisposable
{
    /// <summary>Which strategy this is.</summary>
    ContenderKind Kind { get; }

    /// <summary>False when a local hit cannot be told apart from a network read by synchronous completion; the report then prints the hit ratio as n/a.</summary>
    bool LocalHitsCompleteSynchronously { get; }

    /// <summary>Connects and waits until the instance is ready to serve (for RedisNearCache kinds: <c>Ready</c> completed).</summary>
    Task InitializeAsync(CancellationToken cancellationToken = default);

    /// <summary>Returns the current value of the source-of-truth key as this strategy serves it; <c>default</c> when the key does not exist.</summary>
    ValueTask<T?> GetAsync<T>(string key, CancellationToken cancellationToken = default);

    /// <summary>
    /// Writes the source-of-truth key and updates or invalidates this strategy's own tiers the way the library's documentation
    /// recommends (e.g. <c>HybridCache.SetAsync</c>, <c>FusionCache.SetAsync</c>, which publishes on the backplane).
    /// Completes when the source-of-truth write has been acknowledged by Redis.
    /// </summary>
    ValueTask SetAsync<T>(string key, T value, CancellationToken cancellationToken = default);

    /// <summary>Number of reads (since construction) that loaded from the source-of-truth key over the network: a plain <c>GET</c>, a factory invocation, a RedisNearCache miss.</summary>
    long SourceLoads { get; }
}

/// <summary>
/// Optional: kinds whose local tier can be read without any fallback to L2 or the source (for the post-run staleness audit).
/// Not implemented by <see cref="ContenderKind.Plain"/>.
/// </summary>
public interface ILocalTierPeek
{
    /// <summary>True and the locally held value when this instance's local tier holds <paramref name="key"/>. Never touches the network.</summary>
    bool TryPeekLocal<T>(string key, out T? value);
}
