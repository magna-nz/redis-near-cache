using System.Collections.Concurrent;
using System.Diagnostics;
using System.Globalization;
using System.Net;
using System.Text;
using Azure.Core;
using Microsoft.Azure.StackExchangeRedis;
using RedisNearCache.Tests.EdgeCases;
using RedisNearCache.Tracking.Broadcast;
using StackExchange.Redis;
using Xunit;
using Xunit.Abstractions;

namespace RedisNearCache.Tests.Auth;

/// <summary>
/// The REAL Microsoft Entra ID extension (<c>Microsoft.Azure.StackExchangeRedis</c>) driven against the local
/// <c>redis-near-cache-auth</c> container with a fake <see cref="TokenCredential"/>. This is the CI evidence behind
/// the DESIGN.md "Credentials" claim and the README "EntraID Auth" section:
/// <list type="bullet">
/// <item><description><see cref="ConfigurationOptions.Clone"/> copies <see cref="ConfigurationOptions.Defaults"/> by
/// reference, so the extension's token provider reaches the private multiplexer's two connections AND the Broadcast
/// tracker's own <c>-bcast</c> socket - all three authenticate as the token's <c>oid</c>;</description></item>
/// <item><description>the extension's <c>AfterConnectAsync</c> hook fires for the clone-built private multiplexer, so
/// the extension re-authenticates it when the token rotates (observed through its public
/// <see cref="IAzureCacheTokenEvents.ConnectionReauthenticated"/> event - the private multiplexer is the only
/// multiplexer built from these options in this test, so the event can only be about it);</description></item>
/// <item><description>the library re-authenticates its broadcast socket IN PLACE on a keepalive tick: same client id,
/// no <c>TrackingLost</c>, no <c>Armed</c>, no L1 flush, invalidations never stop.</description></item>
/// </list>
/// </summary>
/// <remarks>
/// <para>Nothing here talks to Azure. <see cref="FakeEntraCredential"/> is the only token source, it hands out
/// locally minted JWT-shaped strings, and every test asserts on its call count, so a real network call would have to
/// come from somewhere the assertions would notice.</para>
/// <para>The server side of the fake is ordinary Redis ACL: the token's <c>oid</c> claim is an ACL user name created
/// at runtime, and the token STRING is that user's password. Redis never validates a JWT - it compares the
/// <c>AUTH</c> password against the user's password list - which is exactly what makes a fake token enough to prove
/// the wiring. Rotation is "add token2 to the user's password list, let the credential start issuing it, then remove
/// token1", so a connection still working after token1 is gone can only be one that moved to token2.</para>
/// <para>Two deliberate deviations from the README snippet, both unavoidable locally and both called out at their
/// site: <c>Ssl</c> is forced off (the extension's provider returns <c>true</c> from <c>GetDefaultSsl</c> for every
/// endpoint), and the token lifetimes are seconds rather than hours.</para>
/// </remarks>
public class EntraIdTests
{
    private readonly ITestOutputHelper _out;

    public EntraIdTests(ITestOutputHelper output) => _out = output;

    // --- the fake credential ---------------------------------------------------------------------------

    /// <summary>
    /// A <see cref="TokenCredential"/> that issues tokens from memory and counts every request. It is the whole
    /// "identity provider" for these tests: no HTTP, no MSAL, no Azure. <see cref="Rotate"/> is what a token refresh
    /// looks like from the extension's point of view.
    /// </summary>
    private sealed class FakeEntraCredential : TokenCredential
    {
        private readonly Lock _gate = new();
        private AccessToken _current;
        private int _calls;

        public FakeEntraCredential(AccessToken first) => _current = first;

        /// <summary>How many times the extension asked this credential for a token.</summary>
        public int Calls => Volatile.Read(ref _calls);

        /// <summary>Every scope set the extension asked for, in order; proves it used the Redis resource scope.</summary>
        public ConcurrentQueue<string> RequestedScopes { get; } = new();

        /// <summary>From now on, hand out <paramref name="next"/>. The server must already accept it.</summary>
        public void Rotate(AccessToken next)
        {
            lock (_gate) _current = next;
        }

        public override AccessToken GetToken(TokenRequestContext requestContext, CancellationToken cancellationToken) =>
            Issue(requestContext);

        public override ValueTask<AccessToken> GetTokenAsync(TokenRequestContext requestContext, CancellationToken cancellationToken) =>
            new(Issue(requestContext));

        private AccessToken Issue(TokenRequestContext requestContext)
        {
            Interlocked.Increment(ref _calls);
            RequestedScopes.Enqueue(string.Join(" ", requestContext.Scopes));
            lock (_gate) return _current;
        }
    }

    // --- JWT-shaped tokens -----------------------------------------------------------------------------

    /// <summary>
    /// The scope <c>AzureCacheOptions.Scope</c> defaults to; asserted so a future package version that changed it
    /// would show up here rather than as a mystery.
    /// </summary>
    private const string RedisScope = "https://redis.azure.com/.default";

    /// <summary>
    /// A token the extension can read: three base64url segments, and a payload carrying the claims it actually uses.
    /// The only claim <c>AzureCacheOptions.GetUserName</c> reads is <c>oid</c> (through
    /// <c>Microsoft.IdentityModel.JsonWebTokens.JsonWebTokenHandler</c>); <c>exp</c>/<c>iat</c>/<c>aud</c>/<c>tid</c>
    /// are there so the value is a plausible token rather than a minimal one. The signature is garbage: nothing in
    /// this path verifies it, and Redis only ever compares the string to an ACL password.
    /// </summary>
    private static string Jwt(string oid, DateTimeOffset expires)
    {
        var header = Segment("""{"alg":"RS256","typ":"JWT","kid":"rnc-test"}""");
        var payload = Segment(string.Create(CultureInfo.InvariantCulture,
            $$"""{"aud":"https://redis.azure.com","iss":"https://sts.windows.net/{{Guid.Empty}}/","oid":"{{oid}}","tid":"{{Guid.Empty}}","iat":{{DateTimeOffset.UtcNow.ToUnixTimeSeconds()}},"exp":{{expires.ToUnixTimeSeconds()}}}"""));
        var signature = Base64Url("this-signature-is-not-real-and-nothing-here-checks-it"u8.ToArray());
        return $"{header}.{payload}.{signature}";

        static string Segment(string json) => Base64Url(Encoding.UTF8.GetBytes(json));
    }

    private static string Base64Url(byte[] bytes) =>
        Convert.ToBase64String(bytes).TrimEnd('=').Replace('+', '-').Replace('/', '_');

    // --- ACL plumbing ----------------------------------------------------------------------------------

    /// <summary>A unique ACL user name per test, used verbatim as the token's <c>oid</c> claim.</summary>
    private static string UniqueOid(string tag) => $"rnc-entra-{tag}-{Guid.NewGuid():N}";

    /// <summary>Creates the ACL user the token names, with the token STRING as its password.</summary>
    private static void CreateTokenUser(string oid, string token) =>
        AuthSupport.Auth("ACL", "SETUSER", oid, "on", $">{token}", "resetchannels", "~*", "&*", "+@all");

    /// <summary>Adds a second valid password (the next token) without invalidating the current one.</summary>
    private static void AddToken(string oid, string token) => AuthSupport.Auth("ACL", "SETUSER", oid, $">{token}");

    /// <summary>Removes one password. Connections already authenticated with it stay up; new ones cannot use it.</summary>
    private static void RemoveToken(string oid, string token) => AuthSupport.Auth("ACL", "SETUSER", oid, $"<{token}");

    /// <summary>Best-effort cleanup: <c>ACL DELUSER</c> on a name that is already gone is a harmless no-op.</summary>
    private static void DeleteTokenUser(string oid)
    {
        try { AuthSupport.Auth("ACL", "DELUSER", oid); }
        catch (InvalidOperationException) { /* nothing left to clean up */ }
    }

    private static void ExternalSet(string key, string value) => AuthSupport.Auth("SET", key, value);

    // --- configuration ---------------------------------------------------------------------------------

    /// <summary>
    /// Exactly the README's "EntraID Auth" call, against the local container.
    /// </summary>
    /// <remarks>
    /// <c>Ssl = false</c> is the one addition: the extension's options provider returns <c>true</c> from
    /// <c>GetDefaultSsl</c> for every endpoint (Entra ID is TLS-only on Azure), and <c>redis-near-cache-auth</c>
    /// speaks plaintext. It is set on the object the caller owns, before the library clones it, so it also proves
    /// that an explicitly set value still beats the provider's default after <see cref="ConfigurationOptions.Clone"/>
    /// (the clone copies the "has value" flags as well as the provider reference).
    /// </remarks>
    private static async Task<ConfigurationOptions> ConfigureAsync(FakeEntraCredential credential)
    {
        var cfg = ConfigurationOptions.Parse(AuthSupport.AuthEndPoint);
        cfg.Ssl = false;
        await cfg.ConfigureForAzureWithTokenCredentialAsync(credential);
        return cfg;
    }

    /// <summary>Every CLIENT LIST line belonging to this cache, the <c>-bcast</c> socket included.</summary>
    private static string[] OurConnections(string clientName) =>
        AuthSupport.Auth("CLIENT", "LIST").Split('\n', StringSplitOptions.RemoveEmptyEntries)
            .Where(line => line.Contains($" name={clientName}", StringComparison.Ordinal)).ToArray();

    // --- 1. the extension end to end, Broadcast --------------------------------------------------------

    /// <summary>
    /// The real extension plus a fake credential: the cache starts, is coherent, caches, invalidates on a foreign
    /// write and removes - and all THREE connections (the private multiplexer's interactive and subscriber pair, and
    /// the Broadcast tracker's hand-rolled RESP3 socket) are authenticated as the token's <c>oid</c>. That last part
    /// is the clone claim: the library never reads a token, it only clones the caller's
    /// <see cref="ConfigurationOptions"/>, and the clone shares the extension's provider.
    /// </summary>
    [Fact]
    public async Task EntraIdTokenCredential_Broadcast_Works()
    {
        var oid = UniqueOid("bcast");
        var expires = DateTimeOffset.UtcNow.AddMinutes(30);
        var token = Jwt(oid, expires);
        CreateTokenUser(oid, token);
        AuthSupport.ResetAclLog(AuthSupport.Auth);

        var credential = new FakeEntraCredential(new AccessToken(token, expires));
        EdgeCaseProvider? handle = null;
        try
        {
            var cfg = await ConfigureAsync(credential);

            // The extension acquired its initial token through the fake, read the oid claim out of it, and installed
            // itself as the options provider. No value was set on cfg.User/cfg.Password: both come from Defaults.
            Assert.Equal(1, credential.Calls);
            Assert.Equal(RedisScope, Assert.Single(credential.RequestedScopes));
            Assert.Equal(oid, cfg.User);
            Assert.Equal(token, cfg.Password);
            Assert.IsAssignableFrom<IAzureCacheTokenEvents>(cfg.Defaults);

            handle = await AuthSupport.BuildAsync(o =>
            {
                o.Configuration = cfg;
                o.TrackingMode = TrackingMode.Broadcast;
                o.KeyPrefixes.Add(AuthSupport.BroadcastPrefix);
            }, captureLogs: true);

            Assert.NotEmpty(handle.Armer.RedirectTargets);
            await AuthSupport.AssertCacheWorksAsync(
                handle.Cache, AuthSupport.BroadcastKey("entra-bcast"), ExternalSet, "entra broadcast");

            var clientName = handle.Connection.ClientName;
            var three = await Poll.UntilAsync(() => OurConnections(clientName).Length == 3, TimeSpan.FromSeconds(10));
            Assert.True(three,
                $"expected 3 connections (multiplexer interactive + subscriber + -bcast); saw:\n{string.Join("\n", OurConnections(clientName))}");
            AuthSupport.AssertConnectedAs(AuthSupport.Auth, clientName, oid, "entra broadcast");
            AuthSupport.AssertNothingDenied(AuthSupport.AclLog(AuthSupport.Auth), handle.LogLines, "entra broadcast");

            // Still exactly one token request: a 30-minute token is nowhere near the extension's 5-minute refresh
            // margin, so nothing went looking for another one.
            Assert.Equal(1, credential.Calls);
            _out.WriteLine($"entra broadcast: {handle.Cache.Statistics}, token requests={credential.Calls}, token length={token.Length}");
        }
        finally
        {
            if (handle is not null) await handle.DisposeAsync();
            DeleteTokenUser(oid);
        }
    }

    // --- 2. rotation -----------------------------------------------------------------------------------

    /// <summary>Token 1's lifetime. Short on purpose: the extension refreshes from the
    /// <c>AzureCacheOptionsProviderWithToken.Password</c> getter the moment the current token is past its expiry, and
    /// the Broadcast tracker reads that getter on every keepalive tick, so an expiry is the one rotation trigger that
    /// does not need the package's 2-minute (internal, non-settable) heartbeat.</summary>
    private static readonly TimeSpan FirstTokenLifetime = TimeSpan.FromSeconds(12);

    /// <summary>
    /// Bound on "rotation visible on the live broadcast socket": the tick that reads an expired token and kicks off
    /// the background refresh, the tick after it that sees the new token and sends <c>AUTH</c>, plus slack.
    /// </summary>
    private static readonly TimeSpan RotationDeadline =
        FirstTokenLifetime + BroadcastTracker.KeepAliveInterval * 3 + TimeSpan.FromSeconds(20);

    /// <summary>
    /// A real token rotation, end to end. Token 2 is added to the ACL user's password list, the credential starts
    /// issuing it, and token 1 expires; from there:
    /// <list type="number">
    /// <item><description>the library re-authenticates its broadcast socket IN PLACE - same client id, no
    /// <c>TrackingLost</c>, no <c>Armed</c>, <c>Statistics.Flushes</c> unchanged, still coherent;</description></item>
    /// <item><description>the extension re-authenticates the clone-built PRIVATE multiplexer - its
    /// <c>ConnectionReauthenticated</c> event fires, which it can only do for a multiplexer that went through its
    /// <c>AfterConnectAsync</c> hook, and the private multiplexer is the only one these options ever built;</description></item>
    /// <item><description>token 1 is then removed from the ACL user, and both a foreign-write eviction and a forced
    /// reconnect of the private multiplexer still work - the reconnect is the strong form, because a connection that
    /// came back up can only have authenticated with token 2.</description></item>
    /// </list>
    /// </summary>
    [Fact]
    public async Task EntraIdTokenRotation_ReauthenticatesInPlace()
    {
        var oid = UniqueOid("rot");
        var now = DateTimeOffset.UtcNow;
        var expiry1 = now + FirstTokenLifetime;
        var expiry2 = now.AddMinutes(30);
        var token1 = Jwt(oid, expiry1);
        var token2 = Jwt(oid, expiry2);
        Assert.NotEqual(token1, token2);
        CreateTokenUser(oid, token1);

        var credential = new FakeEntraCredential(new AccessToken(token1, expiry1));
        var liveKey = AuthSupport.BroadcastKey("entra-muxer-live");
        EdgeCaseProvider? handle = null;
        try
        {
            var cfg = await ConfigureAsync(credential);
            Assert.Equal(oid, cfg.User);

            var events = Assert.IsAssignableFrom<IAzureCacheTokenEvents>(cfg.Defaults);
            var tokenRefreshes = 0;
            var reauthenticated = new ConcurrentQueue<string>();
            var extensionFailures = new ConcurrentQueue<string>();
            events.TokenRefreshed += (_, _) => Interlocked.Increment(ref tokenRefreshes);
            events.ConnectionReauthenticated += (_, endPoint) => reauthenticated.Enqueue(endPoint);
            events.TokenRefreshFailed += (_, e) => extensionFailures.Enqueue($"TokenRefreshFailed: {e.Exception?.Message}");
            events.ConnectionReauthenticationFailed += (_, e) =>
                extensionFailures.Enqueue($"ConnectionReauthenticationFailed({e.Endpoint}): {e.Exception?.Message}");

            handle = await AuthSupport.BuildAsync(o =>
            {
                o.Configuration = cfg;
                o.TrackingMode = TrackingMode.Broadcast;
                o.KeyPrefixes.Add(AuthSupport.BroadcastPrefix);
            }, captureLogs: true);

            var cache = handle.Cache;
            var tracker = (BroadcastTracker)handle.Armer;
            var clientName = handle.Connection.ClientName;
            var endPoint = Assert.Single(tracker.RedirectTargets.Keys);
            var socketBefore = tracker.RedirectTargets[endPoint];
            AuthSupport.AssertConnectedAs(AuthSupport.Auth, clientName, oid, "entra rotation, before");

            var sawTrackingLost = false;
            var sawArmed = false;
            tracker.TrackingLost += ep => { if (Equals(ep, endPoint)) sawTrackingLost = true; };
            tracker.Armed += e => { if (Equals(e.EndPoint, endPoint)) sawArmed = true; };
            var flushesBefore = cache.Statistics.Flushes;
            var rearmsBefore = cache.Statistics.Rearms;

            // The server accepts token 2 BEFORE the credential ever hands it out, exactly as a real rotation does.
            AddToken(oid, token2);
            credential.Rotate(new AccessToken(token2, expiry2));
            var sinceRotation = Stopwatch.StartNew();

            var reauthed = await Poll.UntilAsync(() => tracker.Reauthentications >= 1, RotationDeadline);
            var rotationTook = sinceRotation.Elapsed;
            Assert.True(reauthed,
                $"the broadcast socket was not re-authenticated within {RotationDeadline.TotalSeconds:0} s; " +
                $"tokenRefreshes={tokenRefreshes}, credential calls={credential.Calls}, extension failures=[{string.Join("; ", extensionFailures)}]");
            Assert.Empty(extensionFailures);

            // The extension refreshed the token exactly once, through the fake and nothing else.
            Assert.True(await Poll.UntilAsync(() => Volatile.Read(ref tokenRefreshes) >= 1, TimeSpan.FromSeconds(15)),
                "the extension never raised TokenRefreshed.");
            Assert.Equal(1, Volatile.Read(ref tokenRefreshes));
            Assert.True(credential.Calls >= 2, $"expected at least the initial acquire and one refresh; saw {credential.Calls}.");
            Assert.All(credential.RequestedScopes, scope => Assert.Equal(RedisScope, scope));
            Assert.Equal(token2, cfg.Password);
            Assert.Equal(oid, cfg.User); // the oid is read once, from the first token: rotation changes the password only.

            // In place: same socket, no lifecycle event, no flush, no re-arm, never left coherence.
            Assert.Equal(1, tracker.Reauthentications);
            Assert.Equal(socketBefore, tracker.RedirectTargets[endPoint]);
            Assert.False(sawTrackingLost, "an in-place AUTH must not raise TrackingLost.");
            Assert.False(sawArmed, "an in-place AUTH must not raise Armed.");
            Assert.Equal(flushesBefore, cache.Statistics.Flushes);
            Assert.Equal(rearmsBefore, cache.Statistics.Rearms);
            Assert.True(cache.IsCoherent, "the cache left coherence across the rotation.");

            // The extension re-authenticated the private multiplexer. Only multiplexers that went through its
            // AfterConnectAsync hook are in its list, and the private multiplexer is the only multiplexer these
            // options ever built - so this event IS the hook firing for a clone-built multiplexer.
            // Waited for, not read: the extension's AUTH on the multiplexer is its own round trip, started by the same
            // refresh that handed the tracker the new token, and the tracker's AUTH can finish first (it did on two
            // of six CI jobs).
            Assert.True(await Poll.UntilAsync(() => !reauthenticated.IsEmpty, TimeSpan.FromSeconds(15)),
                "the extension re-authenticated no connection: its AfterConnectAsync hook never registered the private multiplexer. " +
                $"extension failures=[{string.Join("; ", extensionFailures)}]");
            AuthSupport.AssertConnectedAs(AuthSupport.Auth, clientName, oid, "entra rotation, after");

            // (a) Tracking is alive on the re-authenticated socket. Token 1 is gone from here on, so nothing below
            //     can be explained by a connection that quietly kept the old credential and reconnected with it.
            RemoveToken(oid, token1);
            var evictKey = AuthSupport.BroadcastKey("entra-after-rotation");
            await cache.SetAsync(evictKey, "v1");
            Assert.True(await TestHelpers.ReadUntilCachedAsync(cache, evictKey, "v1"), $"{evictKey} never reached L1 after the rotation.");
            ExternalSet(evictKey, "v2");
            Assert.True(await Poll.UntilAsync(() => !cache.TryGetLocal<string>(evictKey, out _), AuthSupport.EvictionDeadline),
                "a foreign write after the rotation did not evict L1: tracking did not survive the in-place AUTH.");
            Assert.Equal("v2", await cache.GetAsync<string>(evictKey));
            Assert.True(await cache.RemoveAsync(evictKey));

            // (b) The private multiplexer still reads and writes with token 1 gone. On its own this is weak - an
            //     established Redis connection stays authenticated whatever happens to the password afterwards - so
            //     the connections are then killed and the same assertions are made about the reconnect, which can
            //     only have authenticated with token 2.
            await cache.SetAsync(liveKey, "live");
            Assert.Equal("live", await cache.GetAsync<string>(liveKey));

            var toKill = EdgeCaseSupport.ClientIdsNamed(AuthSupport.Auth("CLIENT", "LIST"), clientName);
            Assert.Equal(2, toKill.Count); // interactive + subscriber; the -bcast socket has a different name.
            foreach (var id in toKill) AuthSupport.Auth("CLIENT", "KILL", "ID", id.ToString(CultureInfo.InvariantCulture));

            var reconnected = await Poll.UntilAsync(
                () => cache.IsCoherent && EdgeCaseSupport.ClientIdsNamed(AuthSupport.Auth("CLIENT", "LIST"), clientName).Count == 2
                      && !EdgeCaseSupport.ClientIdsNamed(AuthSupport.Auth("CLIENT", "LIST"), clientName).Intersect(toKill).Any(),
                TimeSpan.FromSeconds(30), TimeSpan.FromMilliseconds(250));
            Assert.True(reconnected,
                $"the private multiplexer did not come back with token 1 removed; it could only reconnect with token 2. coherent={cache.IsCoherent}, stats={cache.Statistics}");
            AuthSupport.AssertConnectedAs(AuthSupport.Auth, clientName, oid, "entra rotation, after the kill");
            await AuthSupport.AssertCacheWorksAsync(
                cache, AuthSupport.BroadcastKey("entra-after-kill"), ExternalSet, "entra rotation, after the kill");

            Assert.Empty(extensionFailures);
            Assert.DoesNotContain(handle.LogLines, l => l.Contains("WRONGPASS", StringComparison.OrdinalIgnoreCase));
            _out.WriteLine($"entra rotation: in-place AUTH {rotationTook.TotalSeconds:0.0} s after the credential rotated " +
                           $"(token 1 lifetime {FirstTokenLifetime.TotalSeconds:0} s, keepalive {BroadcastTracker.KeepAliveInterval.TotalSeconds:0} s); " +
                           $"token requests={credential.Calls}, extension re-authentications=[{string.Join(", ", reauthenticated)}]; {cache.Statistics}");
        }
        finally
        {
            if (handle is not null) await handle.DisposeAsync();
            AuthSupport.Auth("DEL", liveKey);
            DeleteTokenUser(oid);
        }
    }

    // --- 3. the extension end to end, Redirect ---------------------------------------------------------

    /// <summary>
    /// The same as <see cref="EntraIdTokenCredential_Broadcast_Works"/> in the default Redirect mode. The README only
    /// documents Broadcast with Entra ID (that is the mode Azure Managed Redis needs), so this is the answer to
    /// "does the extension work with the other mode too": arming, the <c>__redis__:invalidate</c> subscription and
    /// the <c>CLIENT TRACKING ON REDIRECT</c> handshake all run as the token's <c>oid</c> on the two connections
    /// the private multiplexer has.
    /// </summary>
    [Fact]
    public async Task EntraIdTokenCredential_Redirect_Works()
    {
        var oid = UniqueOid("redir");
        var expires = DateTimeOffset.UtcNow.AddMinutes(30);
        var token = Jwt(oid, expires);
        CreateTokenUser(oid, token);
        AuthSupport.ResetAclLog(AuthSupport.Auth);

        var credential = new FakeEntraCredential(new AccessToken(token, expires));
        EdgeCaseProvider? handle = null;
        try
        {
            var cfg = await ConfigureAsync(credential);
            Assert.Equal(oid, cfg.User);

            handle = await AuthSupport.BuildAsync(o =>
            {
                o.Configuration = cfg;
                o.KeyPrefixes.Add(AuthSupport.InsidePrefix);
            }, captureLogs: true);

            Assert.NotEmpty(handle.Armer.RedirectTargets);
            await AuthSupport.AssertCacheWorksAsync(
                handle.Cache, AuthSupport.InsideKey("entra-redirect"), ExternalSet, "entra redirect");

            var clientName = handle.Connection.ClientName;
            var two = await Poll.UntilAsync(() => OurConnections(clientName).Length == 2, TimeSpan.FromSeconds(10));
            Assert.True(two,
                $"expected 2 connections (multiplexer interactive + subscriber); saw:\n{string.Join("\n", OurConnections(clientName))}");
            AuthSupport.AssertConnectedAs(AuthSupport.Auth, clientName, oid, "entra redirect");
            AuthSupport.AssertNothingDenied(AuthSupport.AclLog(AuthSupport.Auth), handle.LogLines, "entra redirect");

            Assert.Equal(1, credential.Calls);
            _out.WriteLine($"entra redirect: {handle.Cache.Statistics}, token requests={credential.Calls}");
        }
        finally
        {
            if (handle is not null) await handle.DisposeAsync();
            DeleteTokenUser(oid);
        }
    }
}
