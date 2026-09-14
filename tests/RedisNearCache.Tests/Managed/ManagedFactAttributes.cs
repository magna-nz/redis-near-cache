using Xunit;

namespace RedisNearCache.Tests.Managed;

/// <summary>
/// Skips a hostname-announcing-cluster test when <c>RNC_REDIS_IMAGE</c> (unset means redis:7.4) names a
/// Redis 6.x image: <c>--cluster-announce-hostname</c>/<c>--cluster-preferred-endpoint-type</c> need Redis
/// 7.0+ or Valkey, and <c>managed-up.sh</c> does not create the hostname-announcing cluster container at all
/// on 6.x, so there is nothing at <c>localhost:7200</c> to connect to.
/// </summary>
public sealed class SkipOnRedis6ImageFact : FactAttribute
{
    public SkipOnRedis6ImageFact()
    {
        var image = Environment.GetEnvironmentVariable("RNC_REDIS_IMAGE");
        if (!string.IsNullOrEmpty(image) && image.StartsWith("redis:6", StringComparison.Ordinal))
        {
            Skip = $"managed-up.sh does not create the hostname-announcing cluster container on {image} " +
                   "(needs Redis 7.0+ or Valkey); unset RNC_REDIS_IMAGE or set it to a newer image to run this test.";
        }
    }
}

/// <summary>
/// Opt-in test against a real external endpoint (AWS ElastiCache, Azure Managed Redis, or anything else you
/// point it at) rather than a Docker container. Skipped unless <c>RNC_EXTERNAL_REDIS</c> is set to a
/// StackExchange.Redis connection string.
/// </summary>
public sealed class ExternalManagedRedisFact : FactAttribute
{
    public ExternalManagedRedisFact()
    {
        if (string.IsNullOrEmpty(Environment.GetEnvironmentVariable("RNC_EXTERNAL_REDIS")))
        {
            Skip = "set RNC_EXTERNAL_REDIS to a StackExchange.Redis connection string to run";
        }
    }
}
