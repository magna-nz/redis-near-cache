using System.Globalization;
using RedisNearCache.Tests.EdgeCases;
using RedisNearCache.Tests.Resilience;
using StackExchange.Redis;
using Xunit;
using Xunit.Abstractions;

namespace RedisNearCache.Tests.Auth;

/// <summary>
/// Sentinel with authentication, against <c>redis-near-cache-auth-sentinel</c>: one password-protected data
/// deployment (master <c>127.0.0.1:6470</c>, replicas 6471-6472) watched by three INDEPENDENT single-sentinel
/// groups that differ only in the sentinel's own <c>requirepass</c> - the same one as the data nodes
/// (<c>authsame</c>, :26390), none at all (<c>authopen</c>, :26391) and a different one (<c>authother</c>,
/// :26392). A Sentinel connection has two hops, sentinel and data node, and <see cref="ConfigurationOptions"/>
/// has exactly one <see cref="ConfigurationOptions.Password"/> for both; these tests establish which of the three
/// production shapes that single field can express.
/// </summary>
/// <remarks>
/// Nothing here triggers a failover: this container's sentinels have quorum 1 and three groups watch one master,
/// so a failover would leave the deployment in a state later runs could not assume.
/// </remarks>
public class SentinelAuthTests
{
    private readonly ITestOutputHelper _out;

    public SentinelAuthTests(ITestOutputHelper output) => _out = output;

    /// <summary>Prompt failure, not a retry ladder: a rejected AUTH is a definitive answer.</summary>
    private const string Prompt = "abortConnect=true,connectRetry=1,connectTimeout=5000";

    private static string Conn(int sentinelPort, string service, string? password)
    {
        var s = $"127.0.0.1:{sentinelPort.ToString(CultureInfo.InvariantCulture)},serviceName={service},{Prompt}";
        return password is null ? s : $"{s},password={password}";
    }

    private static void ExternalSet(string key, string value) =>
        AuthSupport.SentinelData(AuthSupport.SentinelMasterPort, "SET", key, value);

    /// <summary>The master must be 6470 for the foreign writes below to land where the cache is reading.</summary>
    private void AssertMasterIs6470(int sentinelPort, string service, string? sentinelPassword)
    {
        var reply = AuthSupport.Sentinel(sentinelPort, sentinelPassword, "SENTINEL", "get-master-addr-by-name", service);
        Assert.Contains(AuthSupport.SentinelMasterPort.ToString(CultureInfo.InvariantCulture), reply, StringComparison.Ordinal);
        _out.WriteLine($"sentinel {sentinelPort} ({service}) reports master {reply.Replace('\n', ':')}");
    }

    // --- (a) one password valid on both hops ----------------------------------------------------------

    /// <summary>
    /// <c>authsame</c>: the sentinel's <c>requirepass</c> and the data nodes' are the same string, so the single
    /// <c>password=</c> authenticates both hops. Redirect mode works end to end - the private multiplexer resolves
    /// the master through the sentinel, arms it, and a foreign write on 6470 evicts L1.
    /// </summary>
    [Fact]
    public async Task SentinelSamePasswordAsDataNodes_Redirect_Works()
    {
        AssertMasterIs6470(AuthSupport.SentinelSamePort, AuthSupport.SentinelSameService, AuthSupport.DefaultPassword);
        var key = TestHelpers.Key("sentinel-same-redirect");
        var handle = await EdgeCaseSupport.BuildAsync(
            Conn(AuthSupport.SentinelSamePort, AuthSupport.SentinelSameService, AuthSupport.DefaultPassword));
        try
        {
            Assert.NotEmpty(handle.Armer.RedirectTargets);
            await AuthSupport.AssertCacheWorksAsync(handle.Cache, key, ExternalSet, "sentinel authsame, redirect");
            _out.WriteLine($"sentinel authsame redirect: {handle.Cache.Statistics}");
        }
        finally
        {
            await handle.DisposeAsync();
            AuthSupport.SentinelData(AuthSupport.SentinelMasterPort, "DEL", key);
        }
    }

    /// <summary>
    /// The same shape in Broadcast mode: the tracker's RESP3 socket is opened to the endpoint the multiplexer
    /// resolved through the sentinel and authenticates with the same single password
    /// (<c>HELLO 3 AUTH default rnc-default-pw</c>).
    /// </summary>
    [Fact]
    public async Task SentinelSamePasswordAsDataNodes_Broadcast_Works()
    {
        AssertMasterIs6470(AuthSupport.SentinelSamePort, AuthSupport.SentinelSameService, AuthSupport.DefaultPassword);
        var key = AuthSupport.BroadcastKey("sentinel-same-bcast");
        var handle = await EdgeCaseSupport.BuildAsync(
            Conn(AuthSupport.SentinelSamePort, AuthSupport.SentinelSameService, AuthSupport.DefaultPassword),
            o =>
            {
                o.TrackingMode = TrackingMode.Broadcast;
                o.KeyPrefixes.Add(AuthSupport.BroadcastPrefix);
            });
        try
        {
            Assert.NotEmpty(handle.Armer.RedirectTargets);
            await AuthSupport.AssertCacheWorksAsync(handle.Cache, key, ExternalSet, "sentinel authsame, broadcast");
            _out.WriteLine($"sentinel authsame broadcast: {handle.Cache.Statistics}");
        }
        finally
        {
            await handle.DisposeAsync();
            AuthSupport.SentinelData(AuthSupport.SentinelMasterPort, "DEL", key);
        }
    }

    // --- (b) sentinel has no password, data nodes do --------------------------------------------------

    /// <summary>
    /// <c>authopen</c>: a very common production shape - the data nodes need a password, the sentinels are left
    /// open - and it WORKS, in Redirect mode, end to end. Worth pinning down because the wire traffic looks like it
    /// should not: this sentinel rejects the password outright
    /// (<c>ERR AUTH &lt;password&gt; called without any password configured for the default user</c>, verified with
    /// redis-cli against :26391), and yet a single <c>password=</c> aimed at it connects. StackExchange.Redis does
    /// not treat the sentinel hop's rejected <c>AUTH</c> as fatal - a plain
    /// <see cref="ConnectionMultiplexer"/> with the very same options connects too, which the test asserts - so the
    /// second hop still authenticates against the data node and the deployment is usable.
    /// </summary>
    [Fact]
    public async Task SentinelWithoutPassword_WhileDataNodesRequireOne_Redirect_Works()
    {
        AssertMasterIs6470(AuthSupport.SentinelOpenPort, AuthSupport.SentinelOpenService, null);
        var connection = Conn(AuthSupport.SentinelOpenPort, AuthSupport.SentinelOpenService, AuthSupport.DefaultPassword);

        // StackExchange.Redis on its own: the same options, no RedisNearCache. This is the baseline the library
        // inherits; if a future version starts rejecting the passwordless sentinel hop, this fails first and says so.
        var plain = await ConnectionMultiplexer.ConnectAsync(ConfigurationOptions.Parse(connection));
        try
        {
            Assert.True(plain.IsConnected, "plain StackExchange.Redis did not connect through the passwordless sentinel.");
        }
        finally
        {
            await plain.DisposeAsync();
        }

        var key = TestHelpers.Key("sentinel-open-redirect");
        var handle = await EdgeCaseSupport.BuildAsync(connection);
        try
        {
            Assert.NotEmpty(handle.Armer.RedirectTargets);
            Assert.Equal(
                AuthSupport.SentinelMasterPort,
                ResilienceSupport.PortOf(handle.Armer.RedirectTargets.Keys.Single()));
            await AuthSupport.AssertCacheWorksAsync(handle.Cache, key, ExternalSet, "sentinel authopen, redirect");
            _out.WriteLine($"sentinel authopen redirect: {handle.Cache.Statistics}");
        }
        finally
        {
            await handle.DisposeAsync();
            AuthSupport.SentinelData(AuthSupport.SentinelMasterPort, "DEL", key);
        }
    }

    /// <summary>
    /// The same passwordless-sentinel deployment in Broadcast mode. The tracker's RESP3 socket never talks to the
    /// sentinel at all - it is opened to the data endpoint the multiplexer resolved - so the single password is
    /// used exactly where it is valid.
    /// </summary>
    [Fact]
    public async Task SentinelWithoutPassword_WhileDataNodesRequireOne_Broadcast_Works()
    {
        AssertMasterIs6470(AuthSupport.SentinelOpenPort, AuthSupport.SentinelOpenService, null);
        var key = AuthSupport.BroadcastKey("sentinel-open-bcast");
        var handle = await EdgeCaseSupport.BuildAsync(
            Conn(AuthSupport.SentinelOpenPort, AuthSupport.SentinelOpenService, AuthSupport.DefaultPassword),
            o =>
            {
                o.TrackingMode = TrackingMode.Broadcast;
                o.KeyPrefixes.Add(AuthSupport.BroadcastPrefix);
            });
        try
        {
            Assert.NotEmpty(handle.Armer.RedirectTargets);
            await AuthSupport.AssertCacheWorksAsync(handle.Cache, key, ExternalSet, "sentinel authopen, broadcast");
            _out.WriteLine($"sentinel authopen broadcast: {handle.Cache.Statistics}");
        }
        finally
        {
            await handle.DisposeAsync();
            AuthSupport.SentinelData(AuthSupport.SentinelMasterPort, "DEL", key);
        }
    }

    // --- (c) sentinel and data passwords differ -------------------------------------------------------

    /// <summary>
    /// <c>authother</c>: the sentinel's password and the data nodes' differ, and there is only one
    /// <see cref="ConfigurationOptions.Password"/> to hold them. Both choices are tried - the data password (the
    /// sentinel rejects it) and the sentinel password (the data node rejects it) - to show that no single value of
    /// that field can express this deployment. Each attempt is compared against a plain
    /// <see cref="ConnectionMultiplexer"/> with the same options, and the two fail identically
    /// (<c>RedisConnectionException</c> wrapping <c>WRONGPASS</c>, in tens of milliseconds): the limitation is
    /// StackExchange.Redis's single-password Sentinel model, not something RedisNearCache adds. Unlike
    /// <see cref="SentinelWithoutPassword_WhileDataNodesRequireOne_Redirect_Works"/>, a WRONGPASS on the sentinel
    /// hop IS fatal - what StackExchange.Redis tolerates there is a sentinel with no password configured at all.
    /// </summary>
    [Fact]
    public async Task SentinelPasswordDifferentFromDataNodes_IsNotSupported_FailsLoudly()
    {
        AssertMasterIs6470(AuthSupport.SentinelOtherPort, AuthSupport.SentinelOtherService, AuthSupport.SentinelPassword);

        await CompareAsync(
            Conn(AuthSupport.SentinelOtherPort, AuthSupport.SentinelOtherService, AuthSupport.DefaultPassword),
            "sentinel authother, using the DATA password");

        await CompareAsync(
            Conn(AuthSupport.SentinelOtherPort, AuthSupport.SentinelOtherService, AuthSupport.SentinelPassword),
            "sentinel authother, using the SENTINEL password");
    }

    // --- shared probe ---------------------------------------------------------------------------------

    /// <summary>
    /// Runs the same connection string twice - once through a plain <see cref="ConnectionMultiplexer"/> and once
    /// through RedisNearCache - under a deadline, and asserts that both fail loudly and that they fail the same
    /// way. Failing identically is the point: it says the limitation is inherited from StackExchange.Redis's
    /// single-password Sentinel model rather than added by this library.
    /// </summary>
    private async Task CompareAsync(string connectionString, string context)
    {
        var (seRedis, seElapsed) = await AuthSupport.FailsPromptlyAsync(
            async () =>
            {
                var mux = await ConnectionMultiplexer.ConnectAsync(ConfigurationOptions.Parse(connectionString));
                await mux.DisposeAsync();
            },
            TimeSpan.FromSeconds(20),
            $"{context} (plain StackExchange.Redis)");
        _out.WriteLine($"{context}: StackExchange.Redis alone -> {seElapsed.TotalMilliseconds:0} ms, {AuthSupport.Describe(seRedis)}");

        var (cache, cacheElapsed) = await AuthSupport.FailsPromptlyAsync(
            async () =>
            {
                var handle = await EdgeCaseSupport.BuildAsync(connectionString, awaitReady: false);
                try
                {
                    await handle.Cache.Ready;
                }
                finally
                {
                    await handle.DisposeAsync();
                }
            },
            TimeSpan.FromSeconds(20),
            $"{context} (RedisNearCache)");
        _out.WriteLine($"{context}: RedisNearCache -> {cacheElapsed.TotalMilliseconds:0} ms, {AuthSupport.Describe(cache)}");

        Assert.NotNull(seRedis);
        Assert.IsAssignableFrom<RedisConnectionException>(seRedis);
        Assert.NotNull(cache);
        Assert.IsAssignableFrom<RedisConnectionException>(cache);
        Assert.Equal(seRedis!.GetType(), cache!.GetType());
    }
}
