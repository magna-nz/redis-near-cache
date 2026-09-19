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
    public async Task DataContainsAllEightKeysWithTheRightValues()
    {
        var fake = new FakeNearCache { IsCoherent = true };
        fake.Statistics.Hit();
        fake.Statistics.Hit();
        fake.Statistics.Miss();
        fake.Statistics.Invalidation();
        fake.Statistics.Flush();
        fake.Statistics.Rearm();
        fake.Statistics.RaceDiscard();
        var check = new RedisNearCacheHealthCheck(fake);

        var result = await check.CheckHealthAsync(Context());

        Assert.NotNull(result.Data);
        Assert.Equal(8, result.Data!.Count);
        Assert.Equal(true, result.Data["coherent"]);
        Assert.Equal(2L, result.Data["hits"]);
        Assert.Equal(1L, result.Data["misses"]);
        Assert.Equal(1L, result.Data["invalidations"]);
        Assert.Equal(1L, result.Data["flushes"]);
        Assert.Equal(1L, result.Data["rearms"]);
        Assert.Equal(1L, result.Data["raceDiscards"]);
        Assert.Equal(0L, result.Data["l1Entries"]);
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
