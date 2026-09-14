using Microsoft.Extensions.DependencyInjection;
using RedisNearCache.Internal;
using StackExchange.Redis;

namespace RedisNearCache.Tests.EdgeCases;

/// <summary>
/// Helpers shared by the edge-case tests only. Everything general (key generation, polling, redis-cli)
/// comes from the existing helpers in the parent directory; this file adds the two things the edge cases
/// need and nothing else does: talking to the replica container, and building a throwaway provider with
/// non-default <see cref="RedisNearCacheOptions"/>.
/// </summary>
internal static class EdgeCaseSupport
{
    public const string ReplicaContainer = "redis-near-cache-replica";
    public const int ReplicaPort = 6380;

    /// <summary>"master,replica" - the multiplexer sees one master (6379) and one replica (6380).</summary>
    public const string MasterAndReplicaConnectionString = "localhost:6379,127.0.0.1:6380";

    /// <summary>redis-cli inside the replica container, which listens on 6380 rather than the default port.</summary>
    public static string Replica(params string[] args)
    {
        var full = new List<string> { "-p", ReplicaPort.ToString() };
        full.AddRange(args);
        return RedisCli.Run(ReplicaContainer, full.ToArray());
    }

    /// <summary>
    /// A provider built for one test, with its own private multiplexer. The caller owns it and disposes it
    /// (always in a finally: several of these tests leave server-side state behind if they do not).
    /// </summary>
    public static async Task<EdgeCaseProvider> BuildAsync(
        string connectionString,
        Action<RedisNearCacheOptions>? configure = null,
        bool awaitReady = true)
    {
        var services = new ServiceCollection();
        services.AddRedisNearCache(connectionString, o => configure?.Invoke(o));
        var provider = services.BuildServiceProvider();
        var handle = Create(provider);
        if (awaitReady) await handle.Cache.Ready;
        return handle;
    }

    /// <summary>Same, but for tests that need to set <see cref="RedisNearCacheOptions.Configuration"/> directly.</summary>
    public static async Task<EdgeCaseProvider> BuildAsync(
        Action<RedisNearCacheOptions> configure,
        bool awaitReady = true)
    {
        var services = new ServiceCollection();
        services.AddRedisNearCache(configure);
        var provider = services.BuildServiceProvider();
        var handle = Create(provider);
        if (awaitReady) await handle.Cache.Ready;
        return handle;
    }

    private static EdgeCaseProvider Create(ServiceProvider provider) => new(
        provider,
        provider.GetRequiredService<IRedisNearCache>(),
        provider.GetRequiredService<RedisNearCacheConnection>(),
        provider.GetRequiredService<ITrackingArmer>());

    /// <summary>
    /// Client ids of every connection of <paramref name="clientName"/> currently open on one server, read
    /// with redis-cli (i.e. not through the multiplexer under test, which would be a moving target while
    /// its own connections are being killed).
    /// </summary>
    /// <summary>Major version of the standalone server (Redis or Valkey), from INFO server.</summary>
    public static int ServerMajorVersion()
    {
        var info = RedisCli.Standalone("INFO", "server");
        foreach (var line in info.Split('\n'))
        {
            var t = line.Trim();
            if (t.StartsWith("redis_version:", StringComparison.Ordinal) || t.StartsWith("valkey_version:", StringComparison.Ordinal))
            {
                var v = t[(t.IndexOf(':') + 1)..];
                if (int.TryParse(v.Split('.')[0], out var major)) return major;
            }
        }
        return 0;
    }

    public static IReadOnlyList<long> ClientIdsNamed(string clientList, string clientName)
    {
        var ids = new List<long>();
        foreach (var line in clientList.Split('\n', StringSplitOptions.RemoveEmptyEntries))
        {
            if (!line.Contains($" name={clientName} ", StringComparison.Ordinal)) continue;
            var idField = line.Split(' ', StringSplitOptions.RemoveEmptyEntries)
                .FirstOrDefault(f => f.StartsWith("id=", StringComparison.Ordinal));
            if (idField is not null && long.TryParse(idField.AsSpan(3), out var id)) ids.Add(id);
        }

        return ids;
    }
}

internal sealed class EdgeCaseProvider(
    ServiceProvider provider,
    IRedisNearCache cache,
    RedisNearCacheConnection connection,
    ITrackingArmer armer) : IAsyncDisposable
{
    public ServiceProvider Provider { get; } = provider;
    public IRedisNearCache Cache { get; } = cache;
    public RedisNearCacheConnection Connection { get; } = connection;
    public ITrackingArmer Armer { get; } = armer;

    public IConnectionMultiplexer Multiplexer => Connection.Multiplexer;

    public ValueTask DisposeAsync() => Provider.DisposeAsync();
}
