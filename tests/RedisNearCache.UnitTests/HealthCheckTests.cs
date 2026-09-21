using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Xunit;

namespace RedisNearCache.UnitTests;

/// <summary><see cref="RedisNearCacheHealthCheck"/> against a small fake, no Redis.</summary>
public class HealthCheckTests
{
    /// <summary>
    /// A minimal fake with a settable <see cref="IsCoherent"/>; the fake in
    /// <c>DistributedCacheAdapterTests.FakeNearCache</c> hardcodes it to true, so a new one is needed here.
    /// </summary>
    private sealed class FakeNearCache : IRedisNearCache
    {
        public bool IsCoherent { get; set; } = true;
        public RedisNearCacheStatistics Statistics { get; } = new();
        public Task Ready => Task.CompletedTask;

        public ValueTask<T?> GetAsync<T>(string key, CancellationToken ct = default) => default;
        public ValueTask<byte[]?> GetBytesAsync(string key, CancellationToken ct = default) => default;
        public ValueTask SetAsync<T>(string key, T value, TimeSpan? expiry = null, CancellationToken ct = default) => default;
        public ValueTask SetBytesAsync(string key, ReadOnlyMemory<byte> value, TimeSpan? expiry = null, CancellationToken ct = default) => default;
        public ValueTask<bool> RemoveAsync(string key, CancellationToken ct = default) => new(false);
        public void EvictLocal(string key) { }
        public void EvictAllLocal() { }
        public bool TryGetLocal<T>(string key, out T? value) { value = default; return false; }
        public Task WaitForCoherenceAsync(CancellationToken ct = default) => Task.CompletedTask;
        public ValueTask DisposeAsync() => default;
    }

    private static HealthCheckContext Context(HealthStatus failureStatus = HealthStatus.Unhealthy) => new()
    {
        // HealthCheckRegistration needs a factory; the health check under test is constructed directly in each
        // test and invoked directly, never through this factory, so it is never called.
        Registration = new HealthCheckRegistration("redis-near-cache", _ => throw new InvalidOperationException("not used"), failureStatus, tags: null),
    };

    [Fact]
    public async Task CoherentIsHealthy()
    {
        var fake = new FakeNearCache { IsCoherent = true };
        var check = new RedisNearCacheHealthCheck(fake);

        var result = await check.CheckHealthAsync(Context());

        Assert.Equal(HealthStatus.Healthy, result.Status);
    }

    [Fact]
    public async Task NotCoherentIsDegradedEvenWithUnhealthyFailureStatus()
    {
        var fake = new FakeNearCache { IsCoherent = false };
        var check = new RedisNearCacheHealthCheck(fake);

        var result = await check.CheckHealthAsync(Context(HealthStatus.Unhealthy));

        Assert.Equal(HealthStatus.Degraded, result.Status);
    }

    [Fact]
    public async Task DataContainsEveryStatisticsKeyWithTheRightValues()
    {
        var fake = new FakeNearCache { IsCoherent = true };
        fake.Statistics.Hit();
        fake.Statistics.Hit();
        fake.Statistics.Miss();
        fake.Statistics.Invalidation();
        fake.Statistics.Flush(Internal.FlushReason.Manual);
        fake.Statistics.Rearm(Internal.ArmReason.Manual);
        fake.Statistics.RaceDiscard();
        fake.Statistics.SerializerFailure();
        var check = new RedisNearCacheHealthCheck(fake);

        var result = await check.CheckHealthAsync(Context());

        Assert.NotNull(result.Data);
        Assert.Equal(15, result.Data!.Count);
        Assert.Equal(true, result.Data["coherent"]);
        Assert.Equal(2L, result.Data["hits"]);
        Assert.Equal(1L, result.Data["misses"]);
        Assert.Equal(1L, result.Data["invalidations"]);
        Assert.Equal(1L, result.Data["flushes"]);
        Assert.Equal(1L, result.Data["rearms"]);
        Assert.Equal(1L, result.Data["raceDiscards"]);
        Assert.Equal(0L, result.Data["l1Entries"]);
        Assert.Equal(1L, result.Data["serializerFailures"]);
        // Nothing is attached to this bare statistics object, so everything computed from a cache's own state reads
        // its default - which is the point: the check works against ANY IRedisNearCache, not only the facade.
        Assert.Equal(0d, result.Data["passThroughSeconds"]);
        Assert.Equal(0L, result.Data["lostEndpointCount"]);
        Assert.Equal(false, result.Data["ttlCapAbandoned"]);
        Assert.Equal(false, result.Data["untrackedReadsUnavailable"]);
        Assert.Equal(0L, result.Data["l1StoreRefusals"]);
        Assert.Equal(0L, result.Data["preArmFailures"]);

        // lostEndpoints comes off the concrete facade, so a caller's own implementation must not get the key at all -
        // an empty string here would read as "nothing is lost" when the truth is "unknown".
        Assert.False(result.Data.ContainsKey("lostEndpoints"));
    }

    // --- Registration() ------------------------------------------------------------------------------------

    /// <summary>
    /// The default registration resolves the default <see cref="IRedisNearCache"/> and is named what the XML doc
    /// promises. A stub is registered rather than a real cache: <c>Registration</c>'s job is which service it asks the
    /// provider for, and a real one would need a Redis.
    /// </summary>
    [Fact]
    public async Task RegistrationResolvesTheDefaultInstanceAndIsNamedForIt()
    {
        var expected = new FakeNearCache { IsCoherent = false };
        var services = new ServiceCollection();
        services.AddSingleton<IRedisNearCache>(expected);
        using var provider = services.BuildServiceProvider();

        var registration = RedisNearCacheHealthCheck.Registration();

        Assert.Equal("redis-near-cache", registration.Name);
        Assert.Empty(registration.Tags);
        var check = registration.Factory(provider);
        // Degraded because the stub says so: proof the check is over THIS instance and not a second one. Run under the
        // real registration, whose FailureStatus is the Unhealthy the builder defaults to when null is passed - which
        // the check ignores by design (see NotCoherentIsDegradedEvenWithUnhealthyFailureStatus).
        Assert.Equal(HealthStatus.Unhealthy, registration.FailureStatus);
        var result = await check.CheckHealthAsync(new HealthCheckContext { Registration = registration });
        Assert.Equal(HealthStatus.Degraded, result.Status);
    }

    /// <summary>
    /// With a service key the KEYED instance is resolved - which plain <c>AddCheck&lt;RedisNearCacheHealthCheck&gt;</c>
    /// cannot do, the reason this method exists - and the name carries the key, so two keyed registrations in one
    /// builder cannot collide.
    /// </summary>
    [Fact]
    public async Task RegistrationResolvesTheKeyedInstanceAndNamesCannotCollide()
    {
        var orders = new FakeNearCache { IsCoherent = false };
        var catalogue = new FakeNearCache { IsCoherent = true };
        var services = new ServiceCollection();
        // A default instance as well, to prove the keyed registration does not fall back to it.
        services.AddSingleton<IRedisNearCache>(new FakeNearCache { IsCoherent = true });
        services.AddKeyedSingleton<IRedisNearCache>("orders", orders);
        services.AddKeyedSingleton<IRedisNearCache>("catalogue", catalogue);
        using var provider = services.BuildServiceProvider();

        var first = RedisNearCacheHealthCheck.Registration(serviceKey: "orders", tags: ["ready"]);
        var second = RedisNearCacheHealthCheck.Registration(serviceKey: "catalogue");

        Assert.Equal("redis-near-cache-orders", first.Name);
        Assert.Equal("redis-near-cache-catalogue", second.Name);
        Assert.NotEqual(first.Name, second.Name);
        Assert.Equal(["ready"], first.Tags);

        // The keyed instance, not the default one: "orders" is the only registered cache reporting Degraded.
        Assert.Equal(HealthStatus.Degraded, (await first.Factory(provider).CheckHealthAsync(Context())).Status);
        Assert.Equal(HealthStatus.Healthy, (await second.Factory(provider).CheckHealthAsync(Context())).Status);
    }

    /// <summary>An explicit name wins over both defaults, for a builder that already has its own naming scheme.</summary>
    [Fact]
    public void RegistrationTakesAnExplicitNameOverTheDefaults()
    {
        Assert.Equal("cache", RedisNearCacheHealthCheck.Registration("cache").Name);
        Assert.Equal("cache", RedisNearCacheHealthCheck.Registration("cache", serviceKey: "orders").Name);
    }

    /// <summary>
    /// Nothing is resolved until the builder actually runs the factory, so a registration for a cache that is not
    /// registered fails there rather than at registration time - and it fails loudly, not with a null check.
    /// </summary>
    [Fact]
    public void RegistrationDefersResolutionToTheFactory()
    {
        var services = new ServiceCollection();
        using var provider = services.BuildServiceProvider();

        var registration = RedisNearCacheHealthCheck.Registration();

        Assert.Throws<InvalidOperationException>(() => registration.Factory(provider));
    }

    [Fact]
    public async Task NeverThrowsAndCompletesSynchronouslyEvenWhenCancelled()
    {
        var fake = new FakeNearCache { IsCoherent = false };
        var check = new RedisNearCacheHealthCheck(fake);
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        // The check does no I/O, so it honours cancellation only by not throwing OperationCanceledException:
        // it still returns promptly with a result.
        var result = await check.CheckHealthAsync(Context(), cts.Token);

        Assert.Equal(HealthStatus.Degraded, result.Status);
    }
}
