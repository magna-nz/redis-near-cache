using Microsoft.Extensions.Caching.Distributed;
using Microsoft.Extensions.DependencyInjection;
using RedisNearCache.HybridCache;
using Xunit;

namespace RedisNearCache.UnitTests;

/// <summary>
/// This file exists to fail the BUILD, not just a test, if the new registration methods ever make an EXISTING call
/// shape ambiguous or bind it somewhere else. Every call in <see cref="EveryExistingCallShapeStillCompiles"/> is
/// one an application writes today - including the ones passing a bare <c>null</c>, which are exactly the shapes a
/// new overload can capture. If a later change (an overload of <c>AddRedisNearCache</c> taking a name, a default
/// value added to a parameter, a parameter reordered) makes any of them ambiguous, this file stops compiling before
/// any assertion runs.
/// </summary>
/// <remarks>
/// Nothing here resolves a service: these tests build an <see cref="IServiceCollection"/> and stop, so no Redis
/// connection is ever opened. The assertions on top of the compile-time guard are only that each call registered
/// what it has always registered.
/// </remarks>
public class RegistrationOverloadResolutionTests
{
    private const string Connection = "localhost:6379";

    [Fact]
    public void EveryExistingCallShapeStillCompiles()
    {
        var services = new ServiceCollection();
        Action<RedisNearCacheOptions> configure = _ => { };
        Action<RedisNearCacheOptions>? nullConfigure = null;

        services.AddRedisNearCache(Connection);
        services.AddRedisNearCache(Connection, null);
        services.AddRedisNearCache(Connection, nullConfigure);
        services.AddRedisNearCache(Connection, o => { });
        services.AddRedisNearCache(Connection, configure);
        services.AddRedisNearCache(o => { });
        services.AddRedisNearCache(configure);

        services.AddRedisNearCacheDistributedCache();

        services.AddRedisNearCacheHybridCache();
        services.AddRedisNearCacheHybridCache(null);
        services.AddRedisNearCacheHybridCache(o => { });

        // All of it is TryAdd-based, so the whole sequence above is one registration of each.
        Assert.Single(services, d => !d.IsKeyedService && d.ServiceType == typeof(IRedisNearCache));
        Assert.Single(services, d => d.ServiceType == typeof(IDistributedCache));
        Assert.DoesNotContain(services, d => d.IsKeyedService);
    }

    [Fact]
    public void EveryNewCallShapeCompilesToo()
    {
        var services = new ServiceCollection();
        Action<RedisNearCacheOptions> configure = _ => { };

        services.AddKeyedRedisNearCache("a", configure);
        services.AddKeyedRedisNearCache("a", o => { });
        services.AddKeyedRedisNearCache("b", Connection);
        services.AddKeyedRedisNearCache("b", Connection, null);
        services.AddKeyedRedisNearCache("b", Connection, o => { });

        services.AddRedisNearCacheDistributedCacheFor("a");
        services.AddRedisNearCacheHybridCacheFor("a");
        services.AddRedisNearCacheHybridCacheFor("a", null);
        services.AddRedisNearCacheHybridCacheFor("a", o => { });

        Assert.Equal(
            new object?[] { "a", "b" },
            services.Where(d => d.IsKeyedService && d.ServiceType == typeof(IRedisNearCache)).Select(d => d.ServiceKey).ToArray());
        Assert.Single(services, d => d.ServiceType == typeof(IDistributedCache));
    }

    /// <summary>
    /// The named adapters are separate METHODS rather than overloads taking a name, so an existing
    /// <c>AddRedisNearCacheHybridCache(null)</c> cannot start binding to them. Both shapes compiling side by side in
    /// one method is the guard; the assertion is that the two do not fight over the single
    /// <see cref="IDistributedCache"/> registration - whichever ran first keeps it (TryAdd).
    /// </summary>
    [Fact]
    public void TheNamedAndUnnamedAdaptersCoexistAndTheFirstRegistrationWins()
    {
        var namedFirst = new ServiceCollection();
        namedFirst.AddRedisNearCacheDistributedCacheFor("a");
        namedFirst.AddRedisNearCacheDistributedCache();
        Assert.Single(namedFirst, d => d.ServiceType == typeof(IDistributedCache));

        var unnamedFirst = new ServiceCollection();
        unnamedFirst.AddRedisNearCacheDistributedCache();
        unnamedFirst.AddRedisNearCacheDistributedCacheFor("a");
        Assert.Single(unnamedFirst, d => d.ServiceType == typeof(IDistributedCache));
    }

    [Fact]
    public void TheNamedAdaptersRefuseANullOrEmptyName()
    {
        var services = new ServiceCollection();

        Assert.Throws<ArgumentNullException>(() => services.AddRedisNearCacheDistributedCacheFor(null!));
        Assert.Throws<ArgumentException>(() => services.AddRedisNearCacheDistributedCacheFor(""));
        Assert.Throws<ArgumentNullException>(() => services.AddRedisNearCacheHybridCacheFor(null!));
        Assert.Throws<ArgumentException>(() => services.AddRedisNearCacheHybridCacheFor(""));
        Assert.Throws<ArgumentNullException>(() => ((IServiceCollection)null!).AddRedisNearCacheDistributedCacheFor("a"));
        Assert.Throws<ArgumentNullException>(() => ((IServiceCollection)null!).AddRedisNearCacheHybridCacheFor("a"));
        Assert.Empty(services);
    }
}
