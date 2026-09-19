using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using RedisNearCache.Internal;
using RedisNearCache.Tests.EdgeCases;
using RedisNearCache.Tracking.Broadcast;
using Xunit;

namespace RedisNearCache.Tests.NamedInstances;

/// <summary>
/// <c>AddKeyedRedisNearCache</c> against the real standalone container: a named instance is a whole second cache -
/// its own private multiplexer (its own Redis client name), its own armer and listener, its own L1, statistics and
/// options - living next to the default one in one process and one Redis.
/// </summary>
/// <remarks>
/// Every test owns its provider and disposes it in a <c>finally</c>; every assertion about counters is a delta on a
/// cache the test owns.
/// </remarks>
public class NamedInstanceTests
{
    private const string Connection = StandaloneCacheFixture.ConnectionString;

    private static string ClientNameOf(IServiceProvider provider, string? name) => name is null
        ? provider.GetRequiredService<RedisNearCacheConnection>().ClientName
        : provider.GetRequiredKeyedService<RedisNearCacheConnection>(name).ClientName;

    /// <summary>How many connections one client name currently has open on the standalone server.</summary>
    private static int OpenConnections(string clientName) =>
        EdgeCaseSupport.ClientIdsNamed(RedisCli.Standalone("CLIENT", "LIST"), clientName).Count;

    [Fact]
    public async Task TheDefaultAndTwoNamedInstancesAreThreeSeparateCaches()
    {
        var ns = $"ns-{Guid.NewGuid():N}:";
        var services = new ServiceCollection();
        services.AddRedisNearCache(Connection);
        services.AddKeyedRedisNearCache("a", Connection, o =>
        {
            o.KeyNamespace = ns;
            o.L1SizeLimit = 5;
            o.ClientNamePrefix = "rnc-a";
        });
        services.AddKeyedRedisNearCache("b", Connection, o => o.ClientNamePrefix = "rnc-b");
        var provider = services.BuildServiceProvider();

        var keyForDefault = TestHelpers.Key("named-default");
        var keyForA = TestHelpers.Key("named-a");
        var keyForB = TestHelpers.Key("named-b");
        string defaultClient = "", aClient = "", bClient = "";
        try
        {
            var @default = provider.GetRequiredService<IRedisNearCache>();
            var a = provider.GetRequiredKeyedService<IRedisNearCache>("a");
            var b = provider.GetRequiredKeyedService<IRedisNearCache>("b");
            await Task.WhenAll(@default.Ready, a.Ready, b.Ready);

            // Three distinct instances, and resolving one twice gives the same one back.
            Assert.NotSame(@default, a);
            Assert.NotSame(@default, b);
            Assert.NotSame(a, b);
            Assert.Same(@default, provider.GetRequiredService<IRedisNearCache>());
            Assert.Same(a, provider.GetRequiredKeyedService<IRedisNearCache>("a"));

            // Three private multiplexers, each under its own client name, two connections each.
            defaultClient = ClientNameOf(provider, null);
            aClient = ClientNameOf(provider, "a");
            bClient = ClientNameOf(provider, "b");
            Assert.StartsWith("rnc-", defaultClient, StringComparison.Ordinal);
            Assert.StartsWith("rnc-a-", aClient, StringComparison.Ordinal);
            Assert.StartsWith("rnc-b-", bClient, StringComparison.Ordinal);
            Assert.Equal(3, new HashSet<string>(new[] { defaultClient, aClient, bClient }, StringComparer.Ordinal).Count);
            Assert.Equal(2, OpenConnections(defaultClient));
            Assert.Equal(2, OpenConnections(aClient));
            Assert.Equal(2, OpenConnections(bClient));

            // Statistics move only for the instance that was read.
            var defaultMisses = @default.Statistics.Misses;
            var aMisses = a.Statistics.Misses;
            var bMisses = b.Statistics.Misses;
            RedisCli.Standalone("SET", ns + keyForA, "a1");
            Assert.Equal("a1", await a.GetAsync<string>(keyForA));
            Assert.Equal(aMisses + 1, a.Statistics.Misses);
            Assert.Equal(defaultMisses, @default.Statistics.Misses);
            Assert.Equal(bMisses, b.Statistics.Misses);

            // "a" honours its own KeyNamespace; the other two do not have one.
            await a.SetAsync(keyForA, "a2");
            Assert.Equal("a2", RedisCli.Standalone("GET", ns + keyForA));
            await b.SetAsync(keyForB, "b1");
            Assert.Equal("b1", RedisCli.Standalone("GET", keyForB));
            await @default.SetAsync(keyForDefault, "d1");
            Assert.Equal("d1", RedisCli.Standalone("GET", keyForDefault));

            // Each is invalidated by a foreign write to its OWN key, and by nobody else's.
            Assert.True(await TestHelpers.ReadUntilCachedAsync(a, keyForA, "a2"), "a never cached its key.");
            Assert.True(await TestHelpers.ReadUntilCachedAsync(b, keyForB, "b1"), "b never cached its key.");
            Assert.True(await TestHelpers.ReadUntilCachedAsync(@default, keyForDefault, "d1"), "the default never cached its key.");

            RedisCli.Standalone("SET", ns + keyForA, "a3");
            Assert.True(await Poll.UntilAsync(() => !a.TryGetLocal<string>(keyForA, out _), TimeSpan.FromSeconds(5)),
                "a was not invalidated by a foreign write to its own key.");
            Assert.True(b.TryGetLocal<string>(keyForB, out _), "b was evicted by a write to a's key.");
            Assert.True(@default.TryGetLocal<string>(keyForDefault, out _), "the default was evicted by a write to a's key.");

            RedisCli.Standalone("SET", keyForB, "b2");
            Assert.True(await Poll.UntilAsync(() => !b.TryGetLocal<string>(keyForB, out _), TimeSpan.FromSeconds(5)),
                "b was not invalidated by a foreign write to its own key.");
            Assert.True(@default.TryGetLocal<string>(keyForDefault, out _), "the default was evicted by a write to b's key.");

            RedisCli.Standalone("SET", keyForDefault, "d2");
            Assert.True(await Poll.UntilAsync(() => !@default.TryGetLocal<string>(keyForDefault, out _), TimeSpan.FromSeconds(5)),
                "the default instance was not invalidated by a foreign write to its own key.");

            // A name that was never registered is not a cache.
            Assert.Throws<InvalidOperationException>(() => provider.GetRequiredKeyedService<IRedisNearCache>("nope"));
            Assert.Null(provider.GetKeyedService<IRedisNearCache>("nope"));
        }
        finally
        {
            await provider.DisposeAsync();
            RedisCli.Standalone("DEL", ns + keyForA, keyForB, keyForDefault);
        }

        // Disposing the provider closes all three sets of connections.
        foreach (var clientName in new[] { defaultClient, aClient, bClient })
        {
            var name = clientName;
            Assert.True(await Poll.UntilAsync(() => OpenConnections(name) == 0, TimeSpan.FromSeconds(10)),
                $"connections for {name} were still open after the provider was disposed.");
        }
    }

    /// <summary>A named instance's own <see cref="RedisNearCacheOptions.L1SizeLimit"/> applies to its own L1 only.</summary>
    [Fact]
    public async Task ANamedInstanceHonoursItsOwnL1SizeLimit()
    {
        var services = new ServiceCollection();
        services.AddRedisNearCache(Connection);
        services.AddKeyedRedisNearCache("small", Connection, o => o.L1SizeLimit = 5);
        var provider = services.BuildServiceProvider();
        var keys = Enumerable.Range(0, 40).Select(i => TestHelpers.Key($"named-l1-{i}")).ToArray();
        try
        {
            var small = provider.GetRequiredKeyedService<IRedisNearCache>("small");
            var @default = provider.GetRequiredService<IRedisNearCache>();
            await Task.WhenAll(small.Ready, @default.Ready);

            foreach (var key in keys) await small.SetAsync(key, key);
            foreach (var key in keys)
            {
                Assert.Equal(key, await small.GetAsync<string>(key));
                Assert.Equal(key, await @default.GetAsync<string>(key));
            }

            // MemoryCache compacts on a thread-pool thread, so the limit is met eventually, not on the last store.
            Assert.True(
                await Poll.UntilAsync(() => keys.Count(k => small.TryGetLocal<string>(k, out _)) <= 5, TimeSpan.FromSeconds(5)),
                $"the named instance held {keys.Count(k => small.TryGetLocal<string>(k, out _))} of {keys.Length} keys against its L1SizeLimit of 5.");

            // ...and the default instance, which kept the default limit, held far more of them: the option is
            // per instance, not global.
            var heldByDefault = keys.Count(k => @default.TryGetLocal<string>(k, out _));
            Assert.True(heldByDefault > 5,
                $"the default instance held only {heldByDefault} keys, so the named instance's limit cannot be shown to be its own.");
        }
        finally
        {
            await provider.DisposeAsync();
            foreach (var key in keys) RedisCli.Standalone("DEL", key);
        }
    }

    /// <summary>A process can register only named instances; the unkeyed <see cref="IRedisNearCache"/> is then simply absent.</summary>
    [Fact]
    public async Task ANamedInstanceWithoutADefaultOneWorksAndLeavesTheUnkeyedServiceUnregistered()
    {
        var services = new ServiceCollection();
        services.AddKeyedRedisNearCache("only", Connection);
        var provider = services.BuildServiceProvider();
        var key = TestHelpers.Key("named-only");
        try
        {
            Assert.Null(provider.GetService<IRedisNearCache>());
            Assert.Throws<InvalidOperationException>(() => provider.GetRequiredService<IRedisNearCache>());

            var only = provider.GetRequiredKeyedService<IRedisNearCache>("only");
            await only.Ready;

            await only.SetAsync(key, "v1");
            Assert.Equal("v1", RedisCli.Standalone("GET", key));
            Assert.True(await TestHelpers.ReadUntilCachedAsync(only, key, "v1"));

            RedisCli.Standalone("SET", key, "v2");
            Assert.True(await Poll.UntilAsync(() => !only.TryGetLocal<string>(key, out _), TimeSpan.FromSeconds(5)),
                "the only (named) instance was not invalidated by a foreign write.");
        }
        finally
        {
            await provider.DisposeAsync();
            RedisCli.Standalone("DEL", key);
        }
    }

    /// <summary>
    /// Two named instances over ONE Redis with different <see cref="RedisNearCacheOptions.TrackingMode"/>s: the
    /// mode is read from each instance's own options, so one can be armed with <c>BCAST</c> while the other is
    /// redirected, and both stay coherent.
    /// </summary>
    [Fact]
    public async Task OneNamedInstanceCanBeBroadcastWhileAnotherIsRedirect()
    {
        var broadcastPrefix = $"bc-{Guid.NewGuid():N}:";
        var services = new ServiceCollection();
        services.AddKeyedRedisNearCache("bcast", Connection, o =>
        {
            o.TrackingMode = TrackingMode.Broadcast;
            o.KeyPrefixes.Add(broadcastPrefix);
        });
        services.AddKeyedRedisNearCache("redirect", Connection);
        var provider = services.BuildServiceProvider();
        var broadcastKey = broadcastPrefix + TestHelpers.Key("bc");
        var redirectKey = TestHelpers.Key("rd");
        try
        {
            var bcast = provider.GetRequiredKeyedService<IRedisNearCache>("bcast");
            var redirect = provider.GetRequiredKeyedService<IRedisNearCache>("redirect");
            await Task.WhenAll(bcast.Ready, redirect.Ready);

            // Each instance got the armer its own options asked for.
            Assert.IsType<BroadcastTracker>(provider.GetRequiredKeyedService<ITrackingArmer>("bcast"));
            Assert.IsNotType<BroadcastTracker>(provider.GetRequiredKeyedService<ITrackingArmer>("redirect"));
            Assert.Equal(new[] { broadcastPrefix },
                ((BroadcastTracker)provider.GetRequiredKeyedService<ITrackingArmer>("bcast")).Prefixes.ToArray());

            RedisCli.Standalone("SET", broadcastKey, "v1");
            RedisCli.Standalone("SET", redirectKey, "v1");
            Assert.True(await TestHelpers.ReadUntilCachedAsync(bcast, broadcastKey, "v1"), "the broadcast instance never cached its key.");
            Assert.True(await TestHelpers.ReadUntilCachedAsync(redirect, redirectKey, "v1"), "the redirect instance never cached its key.");

            RedisCli.Standalone("SET", broadcastKey, "v2");
            RedisCli.Standalone("SET", redirectKey, "v2");

            Assert.True(await Poll.UntilAsync(() => !bcast.TryGetLocal<string>(broadcastKey, out _), TimeSpan.FromSeconds(5)),
                "the Broadcast-mode named instance was not invalidated.");
            Assert.True(await Poll.UntilAsync(() => !redirect.TryGetLocal<string>(redirectKey, out _), TimeSpan.FromSeconds(5)),
                "the Redirect-mode named instance was not invalidated.");
            Assert.Equal("v2", await bcast.GetAsync<string>(broadcastKey));
            Assert.Equal("v2", await redirect.GetAsync<string>(redirectKey));
        }
        finally
        {
            await provider.DisposeAsync();
            RedisCli.Standalone("DEL", broadcastKey, redirectKey);
        }
    }

    /// <summary>
    /// Named options are validated at host start, exactly as the default ones are (<c>ValidateOnStart</c>), and
    /// before any Redis connection is attempted: <see cref="IRedisNearCache"/> is never resolved because
    /// <c>StartAsync</c> throws first. Mirrors
    /// <see cref="HealthCheckAndValidationTests.HostStartFailsValidationWithoutOpeningARedisConnection"/>.
    /// </summary>
    [Fact]
    public async Task HostStartFailsValidationForAnInvalidNamedRegistration()
    {
        var builder = Host.CreateApplicationBuilder();
        // Deliberately no Configuration/ConnectionString for the named instance.
        builder.Services.AddKeyedRedisNearCache("bad", _ => { });

        using var host = builder.Build();

        var ex = await Assert.ThrowsAsync<OptionsValidationException>(() => host.StartAsync());
        Assert.Contains(ex.Failures, f =>
            f.Contains(nameof(RedisNearCacheOptions.Configuration)) && f.Contains(nameof(RedisNearCacheOptions.ConnectionString)));
        Assert.Equal("bad", ex.OptionsName);
    }

    /// <summary>
    /// A VALID named registration alongside the default one starts the host without either of them being resolved
    /// eagerly - i.e. the named ValidateOnStart above fails for the right reason, not because named options are
    /// broken in general.
    /// </summary>
    [Fact]
    public async Task HostStartSucceedsForAValidNamedRegistration()
    {
        var builder = Host.CreateApplicationBuilder();
        builder.Services.AddRedisNearCache(Connection);
        builder.Services.AddKeyedRedisNearCache("good", Connection);

        var host = builder.Build();
        try
        {
            await host.StartAsync();

            var named = host.Services.GetRequiredKeyedService<IRedisNearCache>("good");
            var @default = host.Services.GetRequiredService<IRedisNearCache>();
            await Task.WhenAll(named.Ready, @default.Ready);
            Assert.NotSame(named, @default);
            Assert.True(named.IsCoherent);
            Assert.True(@default.IsCoherent);

            await host.StopAsync();
        }
        finally
        {
            // The caches are IAsyncDisposable only, so the container must be torn down asynchronously.
            await ((IAsyncDisposable)host).DisposeAsync();
        }
    }
}
