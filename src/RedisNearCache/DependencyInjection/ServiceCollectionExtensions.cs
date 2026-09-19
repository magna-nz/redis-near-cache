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

    /// <summary>
    /// Adds a second (third, ...) <see cref="IRedisNearCache"/> under <paramref name="name"/>, alongside the default
    /// one from <c>AddRedisNearCache</c> or instead of it: its own options, its own private connection, its own L1
    /// and statistics. Resolve it with <c>[FromKeyedServices(name)] IRedisNearCache</c> or
    /// <c>GetRequiredKeyedService&lt;IRedisNearCache&gt;(name)</c>. Use it for a second Redis deployment, or for
    /// differently tuned caches over one (different <see cref="RedisNearCacheOptions.KeyNamespace"/>,
    /// <see cref="RedisNearCacheOptions.L1SizeLimit"/>, <see cref="RedisNearCacheOptions.TrackingMode"/> ...).
    /// </summary>
    /// <remarks>
    /// <paramref name="name"/> is both the service key and the options name, so
    /// <c>IOptionsMonitor&lt;RedisNearCacheOptions&gt;.Get(name)</c> returns this instance's options, which are
    /// validated like the default ones (at host start under <c>IHost</c>). Calling this twice with one name
    /// registers one instance, configured by both delegates in order (as with <c>AddRedisNearCache</c>). The options
    /// are read once, when the instance is first resolved. Every named instance opens its own connections to Redis, and its metrics carry an
    /// extra <c>rnc.instance</c> tag with the name. Nothing about the default instance changes. One thing to know
    /// about keyed services in general: code that walks the <see cref="IServiceCollection"/> reading
    /// <c>ImplementationType</c>/<c>ImplementationFactory</c> from every descriptor (some older scanning and decorator
    /// libraries) throws on a keyed one.
    /// </remarks>
    public static IServiceCollection AddKeyedRedisNearCache(this IServiceCollection services, string name, Action<RedisNearCacheOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentException.ThrowIfNullOrEmpty(name);
        ArgumentNullException.ThrowIfNull(configure);

        services.AddOptions();
        services.Configure(name, configure);
        // After the caller's delegate: the name is what the registration says it is.
        services.Configure<RedisNearCacheOptions>(name, options => options.InstanceName = name);
        // IsKeyedService first: ImplementationInstance throws on a keyed descriptor.
        if (!services.Any(d => !d.IsKeyedService && d.ImplementationInstance is NamedRedisNearCacheOptionsValidator validator && validator.Name == name))
        {
            services.AddSingleton<IValidateOptions<RedisNearCacheOptions>>(new NamedRedisNearCacheOptionsValidator(name));
        }

        services.AddOptions<RedisNearCacheOptions>(name).ValidateOnStart();

        // The options object for this name, taken from the monitor once. The default instance gets that for free from
        // IOptions<T>; a monitor hands out a NEW object after a configuration reload, and the connection, the tracker
        // and the cache must all be built from the same one (KeyPrefixes and KeyNamespace have to agree between them).
        services.TryAddKeyedSingleton(name, (sp, _) =>
            new NamedRedisNearCacheOptions(sp.GetRequiredService<IOptionsMonitor<RedisNearCacheOptions>>().Get(name)));

        services.TryAddKeyedSingleton(name, (sp, _) =>
            RedisNearCacheConnection.Connect(NamedOptions(sp, name), CreateLogger<RedisNearCacheConnection>(sp)));

        // As for the default instance: in Broadcast mode one object is both the armer and the listener, registered
        // once so both interfaces resolve it; it is never constructed in Redirect mode.
        services.TryAddKeyedSingleton(name, (sp, key) =>
            new BroadcastTracker(sp.GetRequiredKeyedService<RedisNearCacheConnection>(key), NamedOptions(sp, name), CreateLogger<BroadcastTracker>(sp)));

        services.TryAddKeyedSingleton<ITrackingArmer>(name, (sp, key) =>
            NamedOptions(sp, name).TrackingMode == TrackingMode.Broadcast
                ? sp.GetRequiredKeyedService<BroadcastTracker>(key)
                : new TrackingArmer(sp.GetRequiredKeyedService<RedisNearCacheConnection>(key), CreateLogger<TrackingArmer>(sp)));

        services.TryAddKeyedSingleton<IInvalidationListener>(name, (sp, key) =>
            NamedOptions(sp, name).TrackingMode == TrackingMode.Broadcast
                ? sp.GetRequiredKeyedService<BroadcastTracker>(key)
                : new InvalidationListener(sp.GetRequiredKeyedService<RedisNearCacheConnection>(key), CreateLogger<InvalidationListener>(sp)));

        services.TryAddKeyedSingleton<IRedisNearCache>(name, (sp, key) => new RedisNearCache.Caching.RedisNearCache(
            sp.GetRequiredKeyedService<RedisNearCacheConnection>(key),
            sp.GetRequiredKeyedService<ITrackingArmer>(key),
            sp.GetRequiredKeyedService<IInvalidationListener>(key),
            Options.Create(NamedOptions(sp, name)),
            CreateLogger<RedisNearCache.Caching.RedisNearCache>(sp)));

        return services;
    }

    /// <summary>Convenience overload of <see cref="AddKeyedRedisNearCache(IServiceCollection, string, Action{RedisNearCacheOptions})"/> that sets <see cref="RedisNearCacheOptions.ConnectionString"/>.</summary>
    public static IServiceCollection AddKeyedRedisNearCache(this IServiceCollection services, string name, string connectionString, Action<RedisNearCacheOptions>? configure = null)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(connectionString);

        return services.AddKeyedRedisNearCache(name, options =>
        {
            options.ConnectionString = connectionString;
            configure?.Invoke(options);
        });
    }

    private static RedisNearCacheOptions NamedOptions(IServiceProvider serviceProvider, string name) =>
        serviceProvider.GetRequiredKeyedService<NamedRedisNearCacheOptions>(name).Value;

    /// <summary>One named instance's options, resolved once; see where it is registered.</summary>
    private sealed record NamedRedisNearCacheOptions(RedisNearCacheOptions Value);

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
