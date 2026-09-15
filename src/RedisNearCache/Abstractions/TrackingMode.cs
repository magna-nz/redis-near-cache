namespace RedisNearCache;

/// <summary>How RedisNearCache receives the server's invalidation messages.</summary>
public enum TrackingMode
{
    /// <summary>
    /// Default. The private multiplexer speaks RESP2 and every master is armed with
    /// <c>CLIENT TRACKING ON REDIRECT &lt;subscriber&gt; OPTOUT NOLOOP</c>, so invalidations for exactly the keys this
    /// instance has read arrive on the multiplexer's own subscriber connection. For Redis 6+ and Valkey reached
    /// directly: self-hosted, ElastiCache node-based, Azure Cache for Redis.
    /// </summary>
    Redirect,

    /// <summary>
    /// For Redis Enterprise-based services (Azure Managed Redis, Redis Cloud, Redis Software), whose proxy rejects
    /// RESP2 tracking and <c>REDIRECT</c>. RedisNearCache opens one small RESP3 connection of its own per master,
    /// armed with <c>CLIENT TRACKING ON BCAST PREFIX ...</c> for each <see cref="RedisNearCacheOptions.KeyPrefixes"/>
    /// entry, and receives an invalidation push for every write under those prefixes whether or not this instance
    /// holds the key. Reads still go through the private multiplexer. Set <c>KeyPrefixes</c>: with none configured,
    /// every write in the database is broadcast to this client.
    /// </summary>
    Broadcast,
}
