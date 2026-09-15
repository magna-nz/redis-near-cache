using RedisNearCache.Internal;
using StackExchange.Redis;
using StackExchange.Redis.Configuration;
using Xunit;

namespace RedisNearCache.UnitTests;

/// <summary>
/// The mechanism <c>TrackingMode.Broadcast</c> relies on for rotating credentials (Entra ID tokens from
/// Microsoft.Azure.StackExchangeRedis, or any custom provider): StackExchange.Redis resolves <c>User</c>/<c>Password</c>
/// through <c>ConfigurationOptions.Defaults</c>, and the clone RedisNearCache builds shares that provider by reference.
/// </summary>
public class CredentialProviderCloneTests
{
    private sealed class RotatingCredentials : DefaultOptionsProvider
    {
        public string? CurrentUser { get; set; }
        public string? CurrentPassword { get; set; }
        public override string? User => CurrentUser;
        public override string? Password => CurrentPassword;
    }

    [Fact]
    public void DefaultsProviderIsSharedByTheClone()
    {
        var provider = new RotatingCredentials { CurrentUser = "alice", CurrentPassword = "p1" };
        var cfg = ConfigurationOptions.Parse("localhost:6379");
        cfg.Defaults = provider;

        var built = RedisNearCacheConnection.BuildConfiguration(new RedisNearCacheOptions { Configuration = cfg });

        Assert.Same(cfg.Defaults, built.Defaults);
        Assert.Equal("alice", built.User);
        Assert.Equal("p1", built.Password);

        provider.CurrentUser = "bob";
        provider.CurrentPassword = "p2";
        Assert.Equal("bob", built.User);
        Assert.Equal("p2", built.Password);
    }

    [Fact]
    public void ExplicitPasswordShadowsTheProvider()
    {
        var provider = new RotatingCredentials { CurrentUser = "alice", CurrentPassword = "p1" };
        var cfg = ConfigurationOptions.Parse("localhost:6379,password=static");
        cfg.Defaults = provider;

        var built = RedisNearCacheConnection.BuildConfiguration(new RedisNearCacheOptions { Configuration = cfg });

        provider.CurrentPassword = "p2";
        provider.CurrentUser = "bob";
        Assert.Equal("static", built.Password);
        // Shadowing is per field: an explicit password does not pin the user, which still follows the provider.
        Assert.Equal("bob", built.User);
    }
}
