using Microsoft.Extensions.Logging;
using StackExchange.Redis;

namespace RedisNearCache.Internal;

/// <summary>
/// The private multiplexer that RedisNearCache owns. Created from a CLONE of the caller's options with
/// RESP2 and admin mode forced: RESP2 because StackExchange.Redis 3.x swallows RESP3 invalidation pushes
/// and only RESP2 gives us a separate subscriber connection to redirect to; admin mode because every
/// CLIENT TRACKING / CLIENT LIST call is gated behind it. The caller's own multiplexer is never touched.
/// </summary>
internal sealed class RedisNearCacheConnection : IAsyncDisposable
{
    public IConnectionMultiplexer Multiplexer { get; }

    /// <summary>The client name set on every connection of <see cref="Multiplexer"/>; used to find our own entries in CLIENT LIST.</summary>
    public string ClientName { get; }

    private RedisNearCacheConnection(IConnectionMultiplexer multiplexer, string clientName)
    {
        Multiplexer = multiplexer;
        ClientName = clientName;
    }

    public static ConfigurationOptions BuildConfiguration(RedisNearCacheOptions options)
    {
        var source = options.Configuration
            ?? (options.ConnectionString is { Length: > 0 } cs ? ConfigurationOptions.Parse(cs)
                : throw new InvalidOperationException($"{nameof(RedisNearCacheOptions)} needs {nameof(RedisNearCacheOptions.Configuration)} or {nameof(RedisNearCacheOptions.ConnectionString)}."));
        var cfg = source.Clone();
        cfg.Protocol = RedisProtocol.Resp2;
        cfg.AllowAdmin = true;
        cfg.ClientName = $"{options.ClientNamePrefix}-{Guid.NewGuid():N}";
        return cfg;
    }

    /// <summary>Synchronous connect for DI factories.</summary>
    public static RedisNearCacheConnection Connect(RedisNearCacheOptions options, ILogger logger)
    {
        var cfg = BuildConfiguration(options);
        logger.LogInformation("RedisNearCache connecting private multiplexer {ClientName} to {EndPoints} (RESP2, admin)",
            cfg.ClientName, string.Join(",", cfg.EndPoints.Select(e => e.ToString())));
        var mux = ConnectionMultiplexer.Connect(cfg);
        return new RedisNearCacheConnection(mux, cfg.ClientName!);
    }

    public static async Task<RedisNearCacheConnection> ConnectAsync(RedisNearCacheOptions options, ILogger logger, CancellationToken cancellationToken)
    {
        var cfg = BuildConfiguration(options);
        logger.LogInformation("RedisNearCache connecting private multiplexer {ClientName} to {EndPoints} (RESP2, admin)",
            cfg.ClientName, string.Join(",", cfg.EndPoints.Select(e => e.ToString())));
        var mux = await ConnectionMultiplexer.ConnectAsync(cfg).WaitAsync(cancellationToken).ConfigureAwait(false);
        return new RedisNearCacheConnection(mux, cfg.ClientName!);
    }

    /// <summary>Connected masters. Tracking is per node, so callers iterate these.</summary>
    public IEnumerable<IServer> ConnectedMasters() =>
        Multiplexer.GetServers().Where(s => s.IsConnected && !s.IsReplica);

    public async ValueTask DisposeAsync()
    {
        await Multiplexer.DisposeAsync().ConfigureAwait(false);
    }
}
