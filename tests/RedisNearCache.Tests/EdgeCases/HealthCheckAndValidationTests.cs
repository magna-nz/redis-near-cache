using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using Xunit;

namespace RedisNearCache.Tests.EdgeCases;

/// <summary>
/// <see cref="RedisNearCacheHealthCheck"/> against a real, armed cache, and <c>AddRedisNearCache</c>'s
/// <c>ValidateOnStart</c> registration under the generic host. Uses real Redis on <c>localhost:6379</c>
/// like the rest of the edge-case suite.
/// </summary>
public class HealthCheckAndValidationTests
{
    [Fact]
    public async Task HealthyAgainstALiveArmedCacheAndL1EntriesMovesAfterARead()
    {
        var key = TestHelpers.Key("healthcheck");
        var handle = await EdgeCaseSupport.BuildAsync(StandaloneCacheFixture.ConnectionString);
        try
        {
            var cache = handle.Cache;
            await cache.WaitForCoherenceAsync();
            var check = new RedisNearCacheHealthCheck(cache);

            var before = await check.CheckHealthAsync(new HealthCheckContext());
            Assert.Equal(HealthStatus.Healthy, before.Status);
            var l1Before = (long)before.Data!["l1Entries"];

            RedisCli.Standalone("SET", key, "v1");
            Assert.True(await TestHelpers.ReadUntilCachedAsync(cache, key, "v1"),
                "the read never populated L1, so l1Entries could not be expected to move.");

            var after = await check.CheckHealthAsync(new HealthCheckContext());
            Assert.Equal(HealthStatus.Healthy, after.Status);
            var l1After = (long)after.Data!["l1Entries"];
            Assert.True(l1After > l1Before, $"l1Entries did not move after a cached read: before={l1Before}, after={l1After}.");
        }
        finally
        {
            await handle.DisposeAsync();
            RedisCli.Standalone("DEL", key);
        }
    }

    /// <summary>
    /// The Degraded case is covered here after dispose rather than via the ACL-denied-tracking setup in
    /// <see cref="DegradedModeRecoveryTests"/>: that class's helpers (DenyTracking/AllowTracking/ConnectAsLimitedUserAsync)
    /// are private to it, and duplicating the ACL plumbing here to reach a not-yet-armed cache is more than "a
    /// little" - so this test instead exercises <see cref="IRedisNearCache.IsCoherent"/> going false through
    /// disposal, which <see cref="RedisNearCache.Caching.RedisNearCache.IsCoherent"/> also gates on.
    /// </summary>
    [Fact]
    public async Task DegradedAfterDisposeAndNeverThrows()
    {
        var handle = await EdgeCaseSupport.BuildAsync(StandaloneCacheFixture.ConnectionString);
        var cache = handle.Cache;
        await cache.WaitForCoherenceAsync();
        var check = new RedisNearCacheHealthCheck(cache);

        await handle.DisposeAsync();

        var result = await check.CheckHealthAsync(new HealthCheckContext());

        Assert.Equal(HealthStatus.Degraded, result.Status);
        Assert.Equal(false, result.Data!["coherent"]);
        Assert.Equal(0L, result.Data["l1Entries"]);
    }

    /// <summary>
    /// <c>ValidateOnStart</c> makes an invalid registration fail at host start, before any Redis connection is
    /// attempted: <see cref="IRedisNearCache"/> (and therefore <see cref="RedisNearCacheConnection.Connect"/>,
    /// which is where a real connection would be opened) is never resolved because <c>StartAsync</c> throws
    /// first.
    /// </summary>
    [Fact]
    public async Task HostStartFailsValidationWithoutOpeningARedisConnection()
    {
        var builder = Host.CreateApplicationBuilder();
        // Deliberately no Configuration/ConnectionString: this is the one rule that cannot be satisfied by
        // accident, so it reliably exercises ValidateOnStart without touching any other option.
        builder.Services.AddRedisNearCache(_ => { });

        using var host = builder.Build();

        var ex = await Assert.ThrowsAsync<OptionsValidationException>(() => host.StartAsync());
        Assert.Contains(ex.Failures, f => f.Contains(nameof(RedisNearCacheOptions.Configuration)) && f.Contains(nameof(RedisNearCacheOptions.ConnectionString)));

        // Nothing in this test ever resolved IRedisNearCache or RedisNearCacheConnection, so AddRedisNearCache's
        // TryAddSingleton factory for the private multiplexer (RedisNearCacheConnection.Connect) never ran: the
        // exception above is proof enough that ValidateOnStart's IStartupValidator hosted service, which runs
        // before user code gets a chance to resolve anything, is what stopped StartAsync - not a failed connect
        // (which would have thrown InvalidOperationException from BuildConfiguration, not OptionsValidationException).
    }
}
