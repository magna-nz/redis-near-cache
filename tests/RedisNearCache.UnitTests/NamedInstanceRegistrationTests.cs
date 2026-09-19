using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Xunit;

namespace RedisNearCache.UnitTests;

/// <summary>
/// <c>AddKeyedRedisNearCache</c> at the level of the <see cref="IServiceCollection"/> and of
/// <see cref="IOptionsMonitor{TOptions}"/> - never by resolving <see cref="IRedisNearCache"/>, which would open a
/// socket. The first test is the important one: after <c>AddRedisNearCache</c> ALONE the collection is exactly what
/// it was before this feature, keyed descriptors included (there are none), so nothing an existing application
/// resolves, scans or decorates has changed shape.
/// </summary>
public class NamedInstanceRegistrationTests
{
    private const string Connection = "localhost:6379"; // parsed, never connected to: nothing here resolves the cache

    /// <summary>Every descriptor as "(keyed?) ServiceType : Lifetime", in registration order.</summary>
    private static string[] Describe(IServiceCollection services) =>
        services.Select(d => $"{(d.IsKeyedService ? $"[{d.ServiceKey}] " : "")}{Pretty(d.ServiceType)} : {d.Lifetime}").ToArray();

    private static string Pretty(Type type)
    {
        if (!type.IsGenericType) return type.Name;
        var name = type.Name[..type.Name.IndexOf('`')];
        return $"{name}<{string.Join(", ", type.GetGenericArguments().Select(Pretty))}>";
    }

    /// <summary>
    /// The full descriptor list <c>AddRedisNearCache(connectionString)</c> produces, pinned. Written out rather than
    /// counted, so that a change shows WHAT moved. Everything before <c>IValidateOptions</c> is what
    /// <c>services.AddOptions()</c> registers; the tail is RedisNearCache's own.
    /// </summary>
    private static readonly string[] DefaultRegistrationDescriptors =
    [
        "IOptions<TOptions> : Singleton",
        "IOptionsSnapshot<TOptions> : Scoped",
        "IOptionsMonitor<TOptions> : Singleton",
        "IOptionsFactory<TOptions> : Transient",
        "IOptionsMonitorCache<TOptions> : Singleton",
        "IConfigureOptions<RedisNearCacheOptions> : Singleton",
        "IValidateOptions<RedisNearCacheOptions> : Singleton",
        "IStartupValidator : Transient",
        "IConfigureOptions<StartupValidatorOptions> : Transient",
        "RedisNearCacheConnection : Singleton",
        "BroadcastTracker : Singleton",
        "ITrackingArmer : Singleton",
        "IInvalidationListener : Singleton",
        "IRedisNearCache : Singleton",
    ];

    /// <summary>
    /// THE regression guard for an existing user's container: <c>AddRedisNearCache</c> on its own registers exactly
    /// what it always did, and in particular registers nothing keyed - keyed descriptors are the one thing in this
    /// change that older scanning/decorator libraries can choke on, and a user who never calls
    /// <c>AddKeyedRedisNearCache</c> must never see one.
    /// </summary>
    [Fact]
    public void AddRedisNearCacheAloneRegistersExactlyWhatItAlwaysDidAndNothingKeyed()
    {
        var services = new ServiceCollection();
        services.AddRedisNearCache(Connection);

        Assert.Equal(DefaultRegistrationDescriptors, Describe(services));
        Assert.DoesNotContain(services, d => d.IsKeyedService);
    }

    [Fact]
    public void TheConfigureOverloadRegistersTheSameThings()
    {
        var withConnectionString = new ServiceCollection();
        withConnectionString.AddRedisNearCache(Connection);
        var withDelegate = new ServiceCollection();
        withDelegate.AddRedisNearCache(o => o.ConnectionString = Connection);

        Assert.Equal(Describe(withConnectionString), Describe(withDelegate));
    }

    // --- the keyed registration ---------------------------------------------------------------------------------

    private static IEnumerable<ServiceDescriptor> KeyedCaches(IServiceCollection services) =>
        services.Where(d => d.IsKeyedService && d.ServiceType == typeof(IRedisNearCache));

    [Fact]
    public void AKeyedRegistrationAddsAKeyedCacheUnderThatServiceKey()
    {
        var services = new ServiceCollection();
        services.AddKeyedRedisNearCache("a", Connection);

        var keyed = Assert.Single(KeyedCaches(services));
        Assert.Equal("a", keyed.ServiceKey);
        Assert.Equal(ServiceLifetime.Singleton, keyed.Lifetime);
    }

    [Fact]
    public void TheDefaultAndTwoNamesCoexist()
    {
        var services = new ServiceCollection();
        services.AddRedisNearCache(Connection);
        services.AddKeyedRedisNearCache("a", Connection);
        services.AddKeyedRedisNearCache("b", Connection);

        Assert.Single(services, d => !d.IsKeyedService && d.ServiceType == typeof(IRedisNearCache));
        Assert.Equal(new object?[] { "a", "b" }, KeyedCaches(services).Select(d => d.ServiceKey).ToArray());
    }

    [Fact]
    public void RegisteringOneNameTwiceAddsNoSecondKeyedCache()
    {
        var services = new ServiceCollection();
        services.AddKeyedRedisNearCache("a", Connection);
        services.AddKeyedRedisNearCache("a", Connection, o => o.L1SizeLimit = 7);

        Assert.Single(KeyedCaches(services));
    }

    /// <summary>Registering a name never touches the default (unnamed) registrations.</summary>
    [Fact]
    public void AKeyedRegistrationAddsNoUnkeyedCacheOfItsOwn()
    {
        var services = new ServiceCollection();
        services.AddKeyedRedisNearCache("a", Connection);

        Assert.DoesNotContain(services, d => !d.IsKeyedService && d.ServiceType == typeof(IRedisNearCache));
    }

    // --- argument validation ------------------------------------------------------------------------------------

    [Fact]
    public void NullServicesIsRefused()
    {
        Assert.Throws<ArgumentNullException>(() => ((IServiceCollection)null!).AddKeyedRedisNearCache("a", _ => { }));
        Assert.Throws<ArgumentNullException>(() => ((IServiceCollection)null!).AddKeyedRedisNearCache("a", Connection));
    }

    [Fact]
    public void ANullNameIsRefusedWithArgumentNullException()
    {
        var services = new ServiceCollection();

        Assert.Throws<ArgumentNullException>(() => services.AddKeyedRedisNearCache(null!, _ => { }));
        Assert.Throws<ArgumentNullException>(() => services.AddKeyedRedisNearCache(null!, Connection));
        Assert.Empty(services);
    }

    [Fact]
    public void AnEmptyNameIsRefusedWithArgumentException()
    {
        var services = new ServiceCollection();

        Assert.Throws<ArgumentException>(() => services.AddKeyedRedisNearCache("", _ => { }));
        Assert.Throws<ArgumentException>(() => services.AddKeyedRedisNearCache("", Connection));
        Assert.Empty(services);
    }

    [Fact]
    public void ANullConfigureOrConnectionStringIsRefused()
    {
        var services = new ServiceCollection();

        Assert.Throws<ArgumentNullException>(() => services.AddKeyedRedisNearCache("a", (Action<RedisNearCacheOptions>)null!));
        Assert.Throws<ArgumentNullException>(() => services.AddKeyedRedisNearCache("a", (string)null!));
        Assert.Empty(services);
    }

    // --- named options ------------------------------------------------------------------------------------------

    private static IOptionsMonitor<RedisNearCacheOptions> Monitor(IServiceCollection services) =>
        services.BuildServiceProvider().GetRequiredService<IOptionsMonitor<RedisNearCacheOptions>>();

    [Fact]
    public void NamedOptionsReflectTheConfigureDelegateAndCarryTheInstanceName()
    {
        var services = new ServiceCollection();
        services.AddRedisNearCache(Connection);
        services.AddKeyedRedisNearCache("a", Connection, o => { o.KeyNamespace = "a:"; o.L1SizeLimit = 7; });

        var monitor = Monitor(services);

        var named = monitor.Get("a");
        Assert.Equal("a:", named.KeyNamespace);
        Assert.Equal(7, named.L1SizeLimit);
        Assert.Equal(Connection, named.ConnectionString);
        Assert.Equal("a", named.InstanceName);

        // The default instance is untouched, and above all still has no instance name: that is what keeps its
        // metrics carrying exactly the tags they always did.
        var @default = monitor.Get(Options.DefaultName);
        Assert.Null(@default.InstanceName);
        Assert.Null(@default.KeyNamespace);
        Assert.NotSame(named, @default);
    }

    /// <summary>The name a registration was made under wins, even if the caller's delegate sets one itself.</summary>
    [Fact]
    public void TheInstanceNameIsTheRegistrationName()
    {
        var services = new ServiceCollection();
        services.AddKeyedRedisNearCache("a", Connection, o => o.InstanceName = "something-else");

        Assert.Equal("a", Monitor(services).Get("a").InstanceName);
    }

    /// <summary>
    /// The proof that a current user who keeps their OWN named <see cref="RedisNearCacheOptions"/> is not broken:
    /// a name RedisNearCache was never asked about is skipped by every validator it registers, even when the
    /// options under it would fail every rule. Before this change all named options were skipped; now all but the
    /// registered ones are, and "the ones this library registered" is the whole of the difference.
    /// </summary>
    [Fact]
    public void OptionsUnderANameThatWasNeverRegisteredAreNotValidated()
    {
        var services = new ServiceCollection();
        services.AddRedisNearCache(Connection);
        services.AddKeyedRedisNearCache("a", Connection);
        services.Configure<RedisNearCacheOptions>("stranger", o => o.L1SizeLimit = -1);

        var monitor = Monitor(services);

        var stranger = monitor.Get("stranger"); // must not throw
        Assert.Equal(-1, stranger.L1SizeLimit);
        Assert.Null(stranger.InstanceName);
    }

    /// <summary>...while a name that WAS registered is validated, which is the point of registering it.</summary>
    [Fact]
    public void OptionsUnderARegisteredNameAreValidated()
    {
        var services = new ServiceCollection();
        services.AddKeyedRedisNearCache("bad", _ => { }); // no Configuration and no ConnectionString

        var monitor = Monitor(services);

        var ex = Assert.Throws<OptionsValidationException>(() => monitor.Get("bad"));
        Assert.Contains(ex.Failures, f =>
            f.Contains(nameof(RedisNearCacheOptions.Configuration)) && f.Contains(nameof(RedisNearCacheOptions.ConnectionString)));
    }

    /// <summary>
    /// A named registration must not make the DEFAULT options fail: the two are validated independently, and a
    /// default registration that was valid before stays valid next to an invalid named one.
    /// </summary>
    [Fact]
    public void AnInvalidNamedRegistrationDoesNotBreakTheDefaultOptions()
    {
        var services = new ServiceCollection();
        services.AddRedisNearCache(Connection);
        services.AddKeyedRedisNearCache("bad", _ => { });

        var provider = services.BuildServiceProvider();

        var @default = provider.GetRequiredService<IOptions<RedisNearCacheOptions>>().Value;
        Assert.Equal(Connection, @default.ConnectionString);
        Assert.Throws<OptionsValidationException>(
            () => provider.GetRequiredService<IOptionsMonitor<RedisNearCacheOptions>>().Get("bad"));
    }

    /// <summary>
    /// Every caller of the named options gets the SAME instance (the monitor's own cache), which the connection,
    /// the armer, the listener and the cache all rely on: they each ask for it separately.
    /// </summary>
    [Fact]
    public void TheNamedOptionsInstanceIsShared()
    {
        var services = new ServiceCollection();
        services.AddKeyedRedisNearCache("a", Connection);
        var monitor = Monitor(services);

        Assert.Same(monitor.Get("a"), monitor.Get("a"));
    }
}
