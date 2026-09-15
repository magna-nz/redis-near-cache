using Microsoft.Extensions.DependencyInjection;
using RedisNearCache.Internal;
using StackExchange.Redis;
using Xunit;

namespace RedisNearCache.Tests;

/// <summary>
/// Builds a fresh <see cref="IServiceProvider"/> (one private multiplexer, singleton <see cref="IRedisNearCache"/>)
/// against the standalone Redis container. xunit's <see cref="IClassFixture{TFixture}"/> creates one instance of
/// this class per test class, so each test class gets its own cache/connection/statistics, and the fixture logic
/// itself is shared across every test file.
/// </summary>
public class StandaloneCacheFixture : IAsyncLifetime
{
    public const string ConnectionString = "localhost:6379";

    public ServiceProvider Provider { get; private set; } = null!;
    public IRedisNearCache Cache { get; private set; } = null!;
    internal RedisNearCacheConnection Connection { get; private set; } = null!;
    internal ITrackingArmer Armer { get; private set; } = null!;
    internal IInvalidationListener Listener { get; private set; } = null!;

    public async Task InitializeAsync()
    {
        var services = new ServiceCollection();
        services.AddRedisNearCache(ConnectionString, Configure);
        Provider = services.BuildServiceProvider();
        Cache = Provider.GetRequiredService<IRedisNearCache>();
        Connection = Provider.GetRequiredService<RedisNearCacheConnection>();
        Armer = Provider.GetRequiredService<ITrackingArmer>();
        Listener = Provider.GetRequiredService<IInvalidationListener>();
        await Cache.Ready;
    }

    public async Task DisposeAsync() => await Provider.DisposeAsync();

    /// <summary>The one (and only, for standalone) endpoint the private multiplexer talks to.</summary>
    public IServer Server() => Connection.Multiplexer.GetServer(Connection.Multiplexer.GetEndPoints()[0]);

    /// <summary>Extension point for subclasses that need non-default options (e.g. Broadcast tracking mode); a no-op here, matching plain <see cref="AddRedisNearCache(IServiceCollection,string)"/>.</summary>
    protected virtual void Configure(RedisNearCacheOptions options)
    {
    }
}

/// <summary>Same idea as <see cref="StandaloneCacheFixture"/> but against the 3-master cluster.</summary>
public class ClusterCacheFixture : IAsyncLifetime
{
    public const string ConnectionString = "127.0.0.1:7100,127.0.0.1:7101,127.0.0.1:7102";
    public static readonly int[] MasterPorts = [7100, 7101, 7102];

    public ServiceProvider Provider { get; private set; } = null!;
    public IRedisNearCache Cache { get; private set; } = null!;
    internal RedisNearCacheConnection Connection { get; private set; } = null!;
    internal ITrackingArmer Armer { get; private set; } = null!;
    internal IInvalidationListener Listener { get; private set; } = null!;

    public async Task InitializeAsync()
    {
        var services = new ServiceCollection();
        services.AddRedisNearCache(ConnectionString, Configure);
        Provider = services.BuildServiceProvider();
        Cache = Provider.GetRequiredService<IRedisNearCache>();
        Connection = Provider.GetRequiredService<RedisNearCacheConnection>();
        Armer = Provider.GetRequiredService<ITrackingArmer>();
        Listener = Provider.GetRequiredService<IInvalidationListener>();
        await Cache.Ready;
    }

    public async Task DisposeAsync() => await Provider.DisposeAsync();

    public IEnumerable<IServer> Masters() => Connection.ConnectedMasters();

    /// <summary>Extension point for subclasses that need non-default options (e.g. Broadcast tracking mode); a no-op here, matching plain <see cref="AddRedisNearCache(IServiceCollection,string)"/>.</summary>
    protected virtual void Configure(RedisNearCacheOptions options)
    {
    }
}
