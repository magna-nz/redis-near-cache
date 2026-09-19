using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using RedisNearCache.Internal;
using RedisNearCache.Tracking;
using RedisNearCache.Tracking.Broadcast;

namespace RedisNearCache;

/// <summary>Registers <see cref="IRedisNearCache"/> and the private connection, armer and listener it needs.</summary>
public static class ServiceCollectionExtensions
{
    /// <summary>
    /// Adds <see cref="IRedisNearCache"/> as a singleton, together with the private
    /// <see cref="RedisNearCacheConnection"/>, <see cref="ITrackingArmer"/> and <see cref="IInvalidationListener"/>
    /// it depends on. <paramref name="configure"/> must set either
    /// <see cref="RedisNearCacheOptions.Configuration"/> or <see cref="RedisNearCacheOptions.ConnectionString"/>,
    /// and the other <see cref="RedisNearCacheOptions"/> values must be individually valid; otherwise
    /// <c>IOptions&lt;RedisNearCacheOptions&gt;.Value</c> throws <c>OptionsValidationException</c> - at host
    /// start under <c>IHost</c> (whose generic host eagerly validates options registered with
    /// <c>ValidateOnStart</c>), or otherwise the first time <see cref="IRedisNearCache"/> (or the options) is
    /// resolved.
    /// </summary>
    public static IServiceCollection AddRedisNearCache(this IServiceCollection services, Action<RedisNearCacheOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(configure);

        services.AddOptions();
        services.Configure(configure);
        services.TryAddEnumerable(ServiceDescriptor.Singleton<IValidateOptions<RedisNearCacheOptions>, RedisNearCacheOptionsValidator>());
        services.AddOptions<RedisNearCacheOptions>().ValidateOnStart();

        services.TryAddSingleton(sp =>
        {
            var options = sp.GetRequiredService<IOptions<RedisNearCacheOptions>>().Value;
            var logger = CreateLogger<RedisNearCacheConnection>(sp);
            return RedisNearCacheConnection.Connect(options, logger);
        });

        // Broadcast mode: one object is both the armer and the listener, because the socket that is armed is the
        // socket the pushes arrive on. Registered once so both interfaces resolve the same instance; never
        // constructed in Redirect mode.
        services.TryAddSingleton(sp =>
        {
            var connection = sp.GetRequiredService<RedisNearCacheConnection>();
            var options = sp.GetRequiredService<IOptions<RedisNearCacheOptions>>().Value;
            var logger = CreateLogger<BroadcastTracker>(sp);
            return new BroadcastTracker(connection, options, logger);
        });

        services.TryAddSingleton<ITrackingArmer>(sp =>
        {
            if (Mode(sp) == TrackingMode.Broadcast) return sp.GetRequiredService<BroadcastTracker>();
            var connection = sp.GetRequiredService<RedisNearCacheConnection>();
            var logger = CreateLogger<TrackingArmer>(sp);
            return new TrackingArmer(connection, logger);
        });

        services.TryAddSingleton<IInvalidationListener>(sp =>
        {
            if (Mode(sp) == TrackingMode.Broadcast) return sp.GetRequiredService<BroadcastTracker>();
            var connection = sp.GetRequiredService<RedisNearCacheConnection>();
            var logger = CreateLogger<InvalidationListener>(sp);
            return new InvalidationListener(connection, logger);
        });

        services.TryAddSingleton<IRedisNearCache>(sp =>
        {
            var connection = sp.GetRequiredService<RedisNearCacheConnection>();
            var armer = sp.GetRequiredService<ITrackingArmer>();
            var listener = sp.GetRequiredService<IInvalidationListener>();
            var options = sp.GetRequiredService<IOptions<RedisNearCacheOptions>>();
            var logger = CreateLogger<RedisNearCache.Caching.RedisNearCache>(sp);
            return new RedisNearCache.Caching.RedisNearCache(connection, armer, listener, options, logger);
        });

        return services;
    }

    /// <summary>Convenience overload that sets <see cref="RedisNearCacheOptions.ConnectionString"/>.</summary>
    public static IServiceCollection AddRedisNearCache(this IServiceCollection services, string connectionString, Action<RedisNearCacheOptions>? configure = null)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(connectionString);

        return services.AddRedisNearCache(options =>
        {
            options.ConnectionString = connectionString;
            configure?.Invoke(options);
        });
    }

    private static TrackingMode Mode(IServiceProvider serviceProvider) =>
        serviceProvider.GetRequiredService<IOptions<RedisNearCacheOptions>>().Value.TrackingMode;

    /// <summary>
    /// Resolves <see cref="ILoggerFactory"/> defensively: RedisNearCache does not register logging itself, so
    /// a host that never called <c>AddLogging</c> still gets a working (no-op) logger instead of a DI failure.
    /// </summary>
    private static ILogger<T> CreateLogger<T>(IServiceProvider serviceProvider)
    {
        var factory = serviceProvider.GetService<ILoggerFactory>() ?? NullLoggerFactory.Instance;
        return factory.CreateLogger<T>();
    }
}
