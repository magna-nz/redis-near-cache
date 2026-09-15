using Xunit;

namespace RedisNearCache.Tests.Enterprise;

/// <summary>
/// Opt-in test against a real (or locally stood-up, via <c>./enterprise-up.sh</c>) Redis Enterprise-based proxy.
/// Skipped unless <c>RNC_ENTERPRISE_REDIS</c> is set to a StackExchange.Redis connection string (mirrors
/// <see cref="Managed.ExternalManagedRedisFact"/>).
/// </summary>
public sealed class EnterpriseFact : FactAttribute
{
    public EnterpriseFact()
    {
        if (string.IsNullOrEmpty(Environment.GetEnvironmentVariable("RNC_ENTERPRISE_REDIS")))
        {
            Skip = "set RNC_ENTERPRISE_REDIS (e.g. localhost:12000 from ./enterprise-up.sh) to run";
        }
    }
}
