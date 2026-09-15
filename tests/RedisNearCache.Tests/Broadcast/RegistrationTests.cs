using Microsoft.Extensions.DependencyInjection;
using RedisNearCache.Internal;
using RedisNearCache.Tracking;
using RedisNearCache.Tracking.Broadcast;
using Xunit;

namespace RedisNearCache.Tests.Broadcast;

/// <summary>
/// <c>AddRedisNearCache</c> registers <see cref="ITrackingArmer"/> and <see cref="IInvalidationListener"/>
/// differently depending on <see cref="RedisNearCacheOptions.TrackingMode"/> (see
/// src/RedisNearCache/DependencyInjection/ServiceCollectionExtensions.cs): in <see cref="TrackingMode.Broadcast"/>
/// both interfaces resolve to the same <see cref="BroadcastTracker"/> instance (the socket that is armed is the
/// socket the pushes arrive on); in <see cref="TrackingMode.Redirect"/> (default) they resolve to two distinct
/// types.
/// </summary>
public class RegistrationTests
{
    [Fact]
    public async Task BroadcastModeResolvesArmerAndListenerToTheSameBroadcastTrackerInstance()
    {
        var services = new ServiceCollection();
        services.AddRedisNearCache(BroadcastStandaloneCacheFixture.ConnectionString, o =>
        {
            o.TrackingMode = TrackingMode.Broadcast;
            o.KeyPrefixes.Add(BroadcastKey.Prefix);
        });
        var provider = services.BuildServiceProvider();
        try
        {
            var armer = provider.GetRequiredService<ITrackingArmer>();
            var listener = provider.GetRequiredService<IInvalidationListener>();
            Assert.Same(armer, listener);
            Assert.IsType<BroadcastTracker>(armer);

            // Confirm it actually starts, not just that DI wires it up.
            await provider.GetRequiredService<IRedisNearCache>().Ready;
        }
        finally
        {
            await provider.DisposeAsync();
        }
    }

    [Fact]
    public async Task RedirectModeResolvesArmerAndListenerToDistinctTrackingArmerAndInvalidationListener()
    {
        var services = new ServiceCollection();
        services.AddRedisNearCache(BroadcastStandaloneCacheFixture.ConnectionString);
        var provider = services.BuildServiceProvider();
        try
        {
            // Resolve the armer/listener directly without going through IRedisNearCache, so nothing is started
            // and there is nothing that needs Ready before DisposeAsync.
            var armer = provider.GetRequiredService<ITrackingArmer>();
            var listener = provider.GetRequiredService<IInvalidationListener>();
            Assert.IsType<TrackingArmer>(armer);
            Assert.IsType<InvalidationListener>(listener);
            Assert.NotSame(armer, listener);
        }
        finally
        {
            await provider.DisposeAsync();
        }
    }
}
