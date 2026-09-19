using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Xunit;

namespace RedisNearCache.UnitTests;

/// <summary>
/// The order in which the default and the named registrations are made must not matter. It once did:
/// <c>AddRedisNearCache</c> adds its validator with <c>TryAddEnumerable</c>, which skips the add when a descriptor
/// with the same service type AND implementation type is already there - and a named instance's validator used to be
/// the same class. Registered first, it made the default instance lose its validation without a word.
/// </summary>
public class NamedInstanceRegistrationOrderTests
{
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void TheDefaultOptionsAreValidatedWhicheverRegistrationCameFirst(bool namedFirst)
    {
        var services = new ServiceCollection();
        if (namedFirst) services.AddKeyedRedisNearCache("a", "localhost:6379");
        services.AddRedisNearCache(o => o.L1SizeLimit = 0);   // no connection either: two failures
        if (!namedFirst) services.AddKeyedRedisNearCache("a", "localhost:6379");
        using var provider = services.BuildServiceProvider();

        var ex = Assert.Throws<OptionsValidationException>(() => provider.GetRequiredService<IOptions<RedisNearCacheOptions>>().Value);
        Assert.Equal(Options.DefaultName, ex.OptionsName);
        Assert.Contains(ex.Failures, f => f.Contains(nameof(RedisNearCacheOptions.L1SizeLimit), StringComparison.Ordinal));

        // and the named one is still valid and still validated on its own terms
        Assert.Equal("localhost:6379", provider.GetRequiredService<IOptionsMonitor<RedisNearCacheOptions>>().Get("a").ConnectionString);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void TheNamedOptionsAreValidatedWhicheverRegistrationCameFirst(bool namedFirst)
    {
        var services = new ServiceCollection();
        if (namedFirst) services.AddKeyedRedisNearCache("bad", o => { });
        services.AddRedisNearCache("localhost:6379");
        if (!namedFirst) services.AddKeyedRedisNearCache("bad", o => { });
        using var provider = services.BuildServiceProvider();

        Assert.Equal("localhost:6379", provider.GetRequiredService<IOptions<RedisNearCacheOptions>>().Value.ConnectionString);
        var ex = Assert.Throws<OptionsValidationException>(() => provider.GetRequiredService<IOptionsMonitor<RedisNearCacheOptions>>().Get("bad"));
        Assert.Equal("bad", ex.OptionsName);
    }
}
