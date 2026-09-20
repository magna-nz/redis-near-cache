using RedisNearCache.Tests.EdgeCases;
using StackExchange.Redis;
using Xunit;
using Xunit.Abstractions;

namespace RedisNearCache.Tests.Auth;

/// <summary>
/// The standalone <c>redis-near-cache-auth</c> container (<c>localhost:6450</c>): <c>requirepass</c> on the
/// <c>default</c> user plus three ACL users. Proves, in both tracking modes, that the credentials survive the
/// clone <c>RedisNearCacheConnection.BuildConfiguration</c> makes of the caller's options, that the Broadcast
/// tracker's hand-rolled RESP3 socket authenticates with the same ones (<c>HELLO 3 AUTH &lt;user&gt; &lt;pw&gt;</c>),
/// and that a wrong password fails loudly rather than hanging.
/// </summary>
/// <remarks>
/// The <c>rnc-minimal-*</c> tests are the ones that pin down the permission surface: they assert not only that the
/// cache works but that the server denied nothing at all (<c>ACL LOG</c> empty), which covers StackExchange.Redis's
/// own handshake and heartbeat traffic as well as the library's commands.
/// </remarks>
public class PasswordAndAclTests
{
    private readonly ITestOutputHelper _out;

    public PasswordAndAclTests(ITestOutputHelper output) => _out = output;

    private static void ExternalSet(string key, string value) => AuthSupport.Auth("SET", key, value);

    // --- password only (no user) ----------------------------------------------------------------------

    /// <summary>
    /// <c>password=</c> with no <c>user=</c>, Redirect mode: the clone keeps the password, so <c>CLIENT LIST</c>,
    /// <c>CLIENT TRACKING ON REDIRECT</c> and the <c>__redis__:invalidate</c> subscription all authenticate as
    /// <c>default</c>.
    /// </summary>
    [Fact]
    public async Task PasswordOnly_Redirect_Works()
    {
        var handle = await EdgeCaseSupport.BuildAsync(AuthSupport.PasswordOnly(AuthSupport.AuthEndPoint));
        try
        {
            Assert.NotEmpty(handle.Armer.RedirectTargets);
            await AuthSupport.AssertCacheWorksAsync(
                handle.Cache, AuthSupport.InsideKey("pw-redirect"), ExternalSet, "password-only redirect");
            _out.WriteLine($"password-only redirect: {handle.Cache.Statistics}");
        }
        finally
        {
            await handle.DisposeAsync();
        }
    }

    /// <summary>
    /// <c>password=</c> with no <c>user=</c>, Broadcast mode. This is the <c>HELLO 3 AUTH default &lt;pw&gt;</c> wire
    /// form: the broadcast socket has no user to send, and Redis's <c>HELLO ... AUTH</c> takes both arguments, so the
    /// tracker substitutes <c>default</c>. Nothing else in the suite covers that substitution against a server that
    /// actually requires a password.
    /// </summary>
    [Fact]
    public async Task PasswordOnly_Broadcast_Works()
    {
        var handle = await EdgeCaseSupport.BuildAsync(AuthSupport.PasswordOnly(AuthSupport.AuthEndPoint), o =>
        {
            o.TrackingMode = TrackingMode.Broadcast;
            o.KeyPrefixes.Add(AuthSupport.BroadcastPrefix);
        });
        try
        {
            Assert.NotEmpty(handle.Armer.RedirectTargets);
            await AuthSupport.AssertCacheWorksAsync(
                handle.Cache, AuthSupport.BroadcastKey("pw-bcast"), ExternalSet, "password-only broadcast");
            _out.WriteLine($"password-only broadcast: {handle.Cache.Statistics}");
        }
        finally
        {
            await handle.DisposeAsync();
        }
    }

    // --- a fully privileged ACL user ------------------------------------------------------------------

    /// <summary>
    /// <c>user=</c>+<c>password=</c> for an ACL user with <c>~* &amp;* +@all</c>, Redirect mode. The
    /// <c>resetchannels</c> on that user makes <c>&amp;*</c> an explicit grant rather than the Redis 6.2 default.
    /// </summary>
    [Fact]
    public async Task FullAclUser_Redirect_Works()
    {
        var handle = await EdgeCaseSupport.BuildAsync(
            AuthSupport.AsUser(AuthSupport.AuthEndPoint, AuthSupport.FullUser, AuthSupport.FullPassword));
        try
        {
            Assert.NotEmpty(handle.Armer.RedirectTargets);
            await AuthSupport.AssertCacheWorksAsync(
                handle.Cache, AuthSupport.InsideKey("full-redirect"), ExternalSet, "rnc-full redirect");
            _out.WriteLine($"rnc-full redirect: {handle.Cache.Statistics}");
        }
        finally
        {
            await handle.DisposeAsync();
        }
    }

    /// <summary>
    /// The same user in Broadcast mode: <c>HELLO 3 AUTH rnc-full rnc-full-pw</c> on the tracker's own socket, and
    /// the private multiplexer authenticating as the same user.
    /// </summary>
    [Fact]
    public async Task FullAclUser_Broadcast_Works()
    {
        var handle = await EdgeCaseSupport.BuildAsync(
            AuthSupport.AsUser(AuthSupport.AuthEndPoint, AuthSupport.FullUser, AuthSupport.FullPassword), o =>
            {
                o.TrackingMode = TrackingMode.Broadcast;
                o.KeyPrefixes.Add(AuthSupport.BroadcastPrefix);
            });
        try
        {
            Assert.NotEmpty(handle.Armer.RedirectTargets);
            await AuthSupport.AssertCacheWorksAsync(
                handle.Cache, AuthSupport.BroadcastKey("full-bcast"), ExternalSet, "rnc-full broadcast");
            _out.WriteLine($"rnc-full broadcast: {handle.Cache.Statistics}");
        }
        finally
        {
            await handle.DisposeAsync();
        }
    }

    // --- the least-privilege ACL users ----------------------------------------------------------------

    /// <summary>
    /// <c>rnc-minimal-redirect</c>: the narrowest ACL Redirect mode is known to work with (the exact rule string is
    /// in <c>auth-up.sh</c> between the <c>ACL users</c> markers). Exercises every command path the mode has -
    /// arming, the <c>__redis__:invalidate</c> subscription, a cached read, a read OUTSIDE <c>KeyPrefixes</c> (the
    /// <c>MULTI</c> / <c>CLIENT CACHING NO</c> / <c>GET</c> / <c>EXEC</c> transaction), a TTL'd key (the
    /// <c>GET</c>+<c>PTTL</c> fallback), <c>SetAsync</c> and <c>RemoveAsync</c> - and then asserts that the server
    /// denied nothing at all, StackExchange.Redis's own handshake and heartbeat traffic included.
    /// </summary>
    [Fact]
    public async Task MinimalAclUser_Redirect_Works_WithNothingDenied()
    {
        AuthSupport.ResetAclLog(AuthSupport.Auth);
        var handle = await EdgeCaseSupport.BuildAsync(
            AuthSupport.AsUser(AuthSupport.AuthEndPoint, AuthSupport.MinimalRedirectUser, AuthSupport.MinimalPassword),
            o => o.KeyPrefixes.Add(AuthSupport.InsidePrefix),
            captureLogs: true);
        var outside = AuthSupport.OutsideKey("minimal-redirect-outside");
        var ttl = AuthSupport.InsideKey("minimal-redirect-ttl");
        try
        {
            Assert.NotEmpty(handle.Armer.RedirectTargets);
            var cache = handle.Cache;

            await AuthSupport.AssertCacheWorksAsync(
                cache, AuthSupport.InsideKey("minimal-redirect"), ExternalSet, "rnc-minimal-redirect");

            // Outside KeyPrefixes: served through the cache but never stored, via MULTI / CLIENT CACHING NO / GET / EXEC.
            AuthSupport.Auth("SET", outside, "outside");
            Assert.Equal("outside", await cache.GetAsync<string>(outside));
            Assert.False(cache.TryGetLocal<string>(outside, out _), "a key outside KeyPrefixes was cached.");
            var hitsBefore = cache.Statistics.Hits;
            Assert.Equal("outside", await cache.GetAsync<string>(outside));
            Assert.Equal(hitsBefore, cache.Statistics.Hits);

            // A TTL'd key: the read path reads the value and its PTTL so the L1 copy cannot outlive the server's.
            var setexBefore = AuthSupport.CommandCalls(AuthSupport.Auth, "setex");
            await cache.SetAsync(ttl, "ttl-v1", TimeSpan.FromMinutes(5));
            Assert.Equal(setexBefore + 1, AuthSupport.CommandCalls(AuthSupport.Auth, "setex"));
            Assert.True(await TestHelpers.ReadUntilCachedAsync(cache, ttl, "ttl-v1"), "the TTL'd key never reached L1.");
            Assert.True(long.Parse(AuthSupport.Auth("PTTL", ttl)) > 0, "the TTL'd key has no expiry on the server.");

            AuthSupport.AssertConnectedAs(AuthSupport.Auth, handle.Connection.ClientName, AuthSupport.MinimalRedirectUser, "rnc-minimal-redirect");
            AuthSupport.AssertNothingDenied(AuthSupport.AclLog(AuthSupport.Auth), handle.LogLines, "rnc-minimal-redirect");
            _out.WriteLine($"rnc-minimal-redirect: {cache.Statistics}");
        }
        finally
        {
            await handle.DisposeAsync();
            AuthSupport.Auth("DEL", outside, ttl);
        }
    }

    /// <summary>
    /// <c>rnc-minimal-bcast</c>: the narrowest ACL Broadcast mode is known to work with. Covers the same read/write
    /// paths (minus the <c>CLIENT CACHING NO</c> transaction, which Broadcast mode does not send - the reading
    /// connection was never tracked) plus the tracker's own socket: <c>HELLO 3 AUTH</c>, <c>CLIENT ID</c>,
    /// <c>CLIENT TRACKING ON BCAST PREFIX</c>, <c>CLIENT TRACKINGINFO</c> and the <c>PING</c> keepalive. Asserts the
    /// server denied nothing.
    /// </summary>
    [Fact]
    public async Task MinimalAclUser_Broadcast_Works_WithNothingDenied()
    {
        AuthSupport.ResetAclLog(AuthSupport.Auth);
        var handle = await EdgeCaseSupport.BuildAsync(
            AuthSupport.AsUser(AuthSupport.AuthEndPoint, AuthSupport.MinimalBroadcastUser, AuthSupport.MinimalPassword),
            o =>
            {
                o.TrackingMode = TrackingMode.Broadcast;
                o.KeyPrefixes.Add(AuthSupport.BroadcastPrefix);
            },
            captureLogs: true);
        var outside = AuthSupport.OutsideKey("minimal-bcast-outside");
        var ttl = AuthSupport.BroadcastKey("minimal-bcast-ttl");
        try
        {
            Assert.NotEmpty(handle.Armer.RedirectTargets);
            var cache = handle.Cache;

            await AuthSupport.AssertCacheWorksAsync(
                cache, AuthSupport.BroadcastKey("minimal-bcast"), ExternalSet, "rnc-minimal-bcast");

            // Outside KeyPrefixes in Broadcast mode: a plain GET, never stored (the server broadcasts nothing for it).
            AuthSupport.Auth("SET", outside, "outside");
            Assert.Equal("outside", await cache.GetAsync<string>(outside));
            Assert.False(cache.TryGetLocal<string>(outside, out _), "a key outside KeyPrefixes was cached.");

            // Not a whole number of seconds: StackExchange.Redis sends PSETEX for it, and SETEX otherwise (the
            // Redirect test above takes that branch), so a least-privilege user needs both.
            var psetexBefore = AuthSupport.CommandCalls(AuthSupport.Auth, "psetex");
            await cache.SetAsync(ttl, "ttl-v1", TimeSpan.FromMilliseconds(300_500));
            Assert.Equal(psetexBefore + 1, AuthSupport.CommandCalls(AuthSupport.Auth, "psetex"));
            Assert.True(await TestHelpers.ReadUntilCachedAsync(cache, ttl, "ttl-v1"), "the TTL'd key never reached L1.");
            Assert.True(long.Parse(AuthSupport.Auth("PTTL", ttl)) > 0, "the TTL'd key has no expiry on the server.");

            AuthSupport.AssertConnectedAs(AuthSupport.Auth, handle.Connection.ClientName, AuthSupport.MinimalBroadcastUser, "rnc-minimal-bcast");
            AuthSupport.AssertNothingDenied(AuthSupport.AclLog(AuthSupport.Auth), handle.LogLines, "rnc-minimal-bcast");
            _out.WriteLine($"rnc-minimal-bcast: {cache.Statistics}");
        }
        finally
        {
            await handle.DisposeAsync();
            AuthSupport.Auth("DEL", outside, ttl);
        }
    }

    /// <summary>
    /// The one command in the minimal ACLs that a short test cannot reach: the Broadcast tracker's keepalive
    /// <c>PING</c>, sent on its own socket every <see cref="RedisNearCache.Tracking.Broadcast.BroadcastTracker.KeepAliveInterval"/>
    /// (10 s). Without <c>+ping</c> the socket is not merely denied a command - a keepalive whose reply never comes
    /// is how the tracker detects a proxy that silently dropped the connection, so a denied <c>PING</c> would look
    /// like a dead socket and cost a re-arm and an L1 flush. This holds one cache open across more than one tick and
    /// asserts the socket is still the same one, still armed, still invalidating, and that nothing was denied.
    /// </summary>
    [Fact]
    public async Task MinimalAclUser_Broadcast_SurvivesTheKeepAlivePing()
    {
        AuthSupport.ResetAclLog(AuthSupport.Auth);
        var handle = await EdgeCaseSupport.BuildAsync(
            AuthSupport.AsUser(AuthSupport.AuthEndPoint, AuthSupport.MinimalBroadcastUser, AuthSupport.MinimalPassword),
            o =>
            {
                o.TrackingMode = TrackingMode.Broadcast;
                o.KeyPrefixes.Add(AuthSupport.BroadcastPrefix);
            },
            captureLogs: true);
        try
        {
            var endpoint = Assert.Single(handle.Armer.RedirectTargets.Keys);
            var socketBefore = handle.Armer.RedirectTargets[endpoint];
            var flushesBefore = handle.Cache.Statistics.Flushes;

            await Task.Delay(RedisNearCache.Tracking.Broadcast.BroadcastTracker.KeepAliveInterval + TimeSpan.FromSeconds(2));

            Assert.Equal(socketBefore, handle.Armer.RedirectTargets[endpoint]);
            Assert.Equal(flushesBefore, handle.Cache.Statistics.Flushes);
            Assert.True(handle.Cache.IsCoherent, "the cache left coherence while only the keepalive was running.");

            await AuthSupport.AssertCacheWorksAsync(
                handle.Cache, AuthSupport.BroadcastKey("minimal-bcast-keepalive"), ExternalSet,
                "rnc-minimal-bcast after a keepalive tick");

            AuthSupport.AssertConnectedAs(AuthSupport.Auth, handle.Connection.ClientName, AuthSupport.MinimalBroadcastUser, "rnc-minimal-bcast keepalive");
            AuthSupport.AssertNothingDenied(AuthSupport.AclLog(AuthSupport.Auth), handle.LogLines, "rnc-minimal-bcast keepalive");
            _out.WriteLine($"rnc-minimal-bcast keepalive: {handle.Cache.Statistics}");
        }
        finally
        {
            await handle.DisposeAsync();
        }
    }

    // --- wrong password -------------------------------------------------------------------------------

    /// <summary>
    /// A wrong password in Redirect mode fails loudly and promptly. Unlike the standalone container used by
    /// <see cref="Resilience.AclAuthenticationTests.WrongPasswordFailsClearly"/>, this one has a real
    /// <c>requirepass</c>, so an unauthenticated connection is not silently usable as a fully privileged
    /// <c>default</c>. The failure mode excluded is a hang: the private multiplexer is built inside a DI factory
    /// and <see cref="IRedisNearCache.Ready"/> is started from a constructor.
    /// </summary>
    [Fact]
    public async Task WrongPassword_Redirect_FailsLoudlyAndPromptly()
    {
        var (failure, elapsed) = await AuthSupport.FailsPromptlyAsync(
            () => AttemptAsync(o => { }), TimeSpan.FromSeconds(20), "wrong password, redirect");

        _out.WriteLine($"wrong password redirect: {elapsed.TotalMilliseconds:0} ms, {AuthSupport.Describe(failure)}");
        Assert.NotNull(failure);
        Assert.IsAssignableFrom<RedisConnectionException>(failure);
    }

    /// <summary>
    /// The same in Broadcast mode. The private multiplexer is built from the same credentials and is rejected
    /// first, so the broadcast socket is never opened: the observable outcome is identical, a
    /// <see cref="RedisConnectionException"/> out of the very first resolve. Worth its own test because a future
    /// change that made the multiplexer lazy would have to keep the broadcast socket's failure just as loud.
    /// </summary>
    [Fact]
    public async Task WrongPassword_Broadcast_FailsLoudlyAndPromptly()
    {
        var (failure, elapsed) = await AuthSupport.FailsPromptlyAsync(
            () => AttemptAsync(o =>
            {
                o.TrackingMode = TrackingMode.Broadcast;
                o.KeyPrefixes.Add(AuthSupport.BroadcastPrefix);
            }),
            TimeSpan.FromSeconds(20),
            "wrong password, broadcast");

        _out.WriteLine($"wrong password broadcast: {elapsed.TotalMilliseconds:0} ms, {AuthSupport.Describe(failure)}");
        Assert.NotNull(failure);
        Assert.IsAssignableFrom<RedisConnectionException>(failure);
    }

    /// <summary>
    /// One connection attempt with a password the server rejects. A rejected <c>AUTH</c> is a definitive answer, so
    /// retrying it only burns the deadline: <c>ConnectRetry=1</c> and a short <c>ConnectTimeout</c> keep the test
    /// measuring the failure rather than the retry ladder.
    /// </summary>
    private static async Task AttemptAsync(Action<RedisNearCacheOptions> configure)
    {
        var options = ConfigurationOptions.Parse(AuthSupport.AuthEndPoint);
        options.Password = "not-the-password";
        options.AbortOnConnectFail = true;
        options.ConnectRetry = 1;
        options.ConnectTimeout = 5_000;

        // Resolving IRedisNearCache builds the private multiplexer synchronously, so this may throw here; if a
        // future change makes the connect lazy, the fault surfaces from Ready instead. Either is a loud failure.
        var handle = await EdgeCaseSupport.BuildAsync(o =>
        {
            o.Configuration = options;
            configure(o);
        }, awaitReady: false);
        try
        {
            await handle.Cache.Ready;
        }
        finally
        {
            await handle.DisposeAsync();
        }
    }
}
