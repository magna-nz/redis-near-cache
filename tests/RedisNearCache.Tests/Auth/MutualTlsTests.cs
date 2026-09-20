using System.Net.Security;
using System.Security.Cryptography.X509Certificates;
using RedisNearCache.Tests.EdgeCases;
using RedisNearCache.Tests.Resilience;
using StackExchange.Redis;
using Xunit;
using Xunit.Abstractions;

namespace RedisNearCache.Tests.Auth;

/// <summary>
/// The TLS-only, client-certificate-requiring container <c>redis-near-cache-mtls</c> (<c>localhost:6460</c>,
/// <c>--tls-auth-clients yes</c>, no password). Both halves of mutual TLS have to reach the connection: the
/// server certificate is signed by the test CA that no machine trust store knows, and the client certificate is
/// demanded by the server. There are two ways to supply them through <see cref="ConfigurationOptions"/> and they
/// do not reach the same places:
/// <list type="bullet">
/// <item>the <c>CertificateValidation</c> / <c>CertificateSelection</c> EVENTS, which only StackExchange.Redis can
/// read (a clone copies them, so the private multiplexer has them);</item>
/// <item><see cref="ConfigurationOptions.SslClientAuthenticationOptions"/>, which is an ordinary property and is
/// therefore the only channel the Broadcast tracker's hand-rolled RESP3 socket can use.</item>
/// </list>
/// That asymmetry is a documented limitation of Broadcast mode; the test named
/// <see cref="Broadcast_ClientCertificateOnlyThroughTheEvent_IsNotSupported_FailsLoudly"/> pins down what it
/// actually does.
/// </summary>
public class MutualTlsTests
{
    private readonly ITestOutputHelper _out;

    public MutualTlsTests(ITestOutputHelper output) => _out = output;

    private static void ExternalSet(string key, string value) => AuthSupport.Mtls("SET", key, value);

    /// <summary>
    /// The trust decision, shared by both supply mechanisms: the presented leaf must chain to the test CA and
    /// nothing but a self-signed chain may be forgiven, so a wrong host name or a missing certificate still fails.
    /// Mirrors <see cref="ResilienceSupport.TlsConfiguration"/>.
    /// </summary>
    private static bool ServerCertificateIsOurs(X509Certificate2 ca, X509Certificate? presented, SslPolicyErrors errors)
    {
        if (presented is null) return false;
        using var leaf = X509CertificateLoader.LoadCertificate(presented.GetRawCertData());
        return ResilienceSupport.ChainsTo(ca, leaf)
               && (errors & ~SslPolicyErrors.RemoteCertificateChainErrors) == SslPolicyErrors.None;
    }

    private static ConfigurationOptions BaseOptions()
    {
        var cfg = new ConfigurationOptions
        {
            Ssl = true,
            SslHost = AuthSupport.MtlsHost,
            AbortOnConnectFail = true,
            ConnectTimeout = 10_000,
            ConnectRetry = 1,
        };
        cfg.EndPoints.Add(AuthSupport.MtlsHost, AuthSupport.MtlsPort);
        return cfg;
    }

    /// <summary>Server trust and the client certificate, both through the StackExchange.Redis events.</summary>
    private static ConfigurationOptions ThroughEvents(X509Certificate2 ca, X509Certificate2? client)
    {
        var cfg = BaseOptions();
        cfg.CertificateValidation += (_, presented, _, errors) => ServerCertificateIsOurs(ca, presented, errors);
        if (client is not null) cfg.CertificateSelection += (_, _, _, _, _) => client;
        return cfg;
    }

    /// <summary>Server trust and the client certificate, both through <c>SslClientAuthenticationOptions</c>.</summary>
    private static ConfigurationOptions ThroughSslOptions(X509Certificate2 ca, X509Certificate2? client)
    {
        var cfg = BaseOptions();
        cfg.SslClientAuthenticationOptions = host => new SslClientAuthenticationOptions
        {
            TargetHost = host,
            ClientCertificates = client is null ? null : [client],
            RemoteCertificateValidationCallback = (_, presented, _, errors) => ServerCertificateIsOurs(ca, presented, errors),
        };
        return cfg;
    }

    // --- Redirect -------------------------------------------------------------------------------------

    /// <summary>
    /// Redirect mode with the client certificate supplied through the <c>CertificateSelection</c> EVENT (and trust
    /// through the <c>CertificateValidation</c> event). Proves the clone
    /// <c>RedisNearCacheConnection.BuildConfiguration</c> makes carries both event handlers: without them the
    /// handshake to a <c>--tls-auth-clients yes</c> server cannot complete at all.
    /// </summary>
    [Fact]
    public async Task Redirect_ClientCertificateThroughTheEvent_Works()
    {
        using var ca = ResilienceSupport.LoadPemCertificate(ResilienceSupport.CaCertPath());
        using var client = AuthSupport.ClientCertificate();
        var key = TestHelpers.Key("mtls-redirect-event");

        var handle = await EdgeCaseSupport.BuildAsync(o => o.Configuration = ThroughEvents(ca, client));
        try
        {
            Assert.NotEmpty(handle.Armer.RedirectTargets);
            await AuthSupport.AssertCacheWorksAsync(handle.Cache, key, ExternalSet, "mTLS redirect via events");
            _out.WriteLine($"mTLS redirect via events: {handle.Cache.Statistics}");
        }
        finally
        {
            await handle.DisposeAsync();
            AuthSupport.Mtls("DEL", key);
        }
    }

    /// <summary>
    /// Redirect mode with both halves supplied through
    /// <see cref="ConfigurationOptions.SslClientAuthenticationOptions"/>. StackExchange.Redis uses that object
    /// verbatim when it is set, so this is the configuration that works in BOTH tracking modes and the one to
    /// reach for when Broadcast mode may be turned on later.
    /// </summary>
    [Fact]
    public async Task Redirect_ClientCertificateThroughSslClientAuthenticationOptions_Works()
    {
        using var ca = ResilienceSupport.LoadPemCertificate(ResilienceSupport.CaCertPath());
        using var client = AuthSupport.ClientCertificate();
        var key = TestHelpers.Key("mtls-redirect-sslopts");

        var handle = await EdgeCaseSupport.BuildAsync(o => o.Configuration = ThroughSslOptions(ca, client));
        try
        {
            Assert.NotEmpty(handle.Armer.RedirectTargets);
            await AuthSupport.AssertCacheWorksAsync(handle.Cache, key, ExternalSet, "mTLS redirect via SslClientAuthenticationOptions");
            _out.WriteLine($"mTLS redirect via SslClientAuthenticationOptions: {handle.Cache.Statistics}");
        }
        finally
        {
            await handle.DisposeAsync();
            AuthSupport.Mtls("DEL", key);
        }
    }

    // --- Broadcast ------------------------------------------------------------------------------------

    /// <summary>
    /// Broadcast mode with both halves through <see cref="ConfigurationOptions.SslClientAuthenticationOptions"/>:
    /// the supported way to run Broadcast mode against a mutual-TLS server. The tracker's own RESP3 socket uses
    /// that object verbatim, so it presents the same client certificate and applies the same trust decision as the
    /// private multiplexer.
    /// </summary>
    [Fact]
    public async Task Broadcast_ClientCertificateThroughSslClientAuthenticationOptions_Works()
    {
        using var ca = ResilienceSupport.LoadPemCertificate(ResilienceSupport.CaCertPath());
        using var client = AuthSupport.ClientCertificate();
        var key = AuthSupport.BroadcastKey("mtls-bcast-sslopts");

        var handle = await EdgeCaseSupport.BuildAsync(o =>
        {
            o.Configuration = ThroughSslOptions(ca, client);
            o.TrackingMode = TrackingMode.Broadcast;
            o.KeyPrefixes.Add(AuthSupport.BroadcastPrefix);
        });
        try
        {
            Assert.NotEmpty(handle.Armer.RedirectTargets);
            await AuthSupport.AssertCacheWorksAsync(handle.Cache, key, ExternalSet, "mTLS broadcast via SslClientAuthenticationOptions");
            _out.WriteLine($"mTLS broadcast via SslClientAuthenticationOptions: {handle.Cache.Statistics}");
        }
        finally
        {
            await handle.DisposeAsync();
            AuthSupport.Mtls("DEL", key);
        }
    }

    /// <summary>
    /// The documented limitation, pinned down: in Broadcast mode a client certificate supplied ONLY through the
    /// <c>CertificateSelection</c> event never reaches the tracker's socket, because that socket is not a
    /// StackExchange.Redis connection and cannot read an event on <see cref="ConfigurationOptions"/>. The private
    /// multiplexer connects (it reads the events), so reads and writes work, but no master can be armed.
    /// The required outcome is the loud one: startup faults, <see cref="IRedisNearCache.IsCoherent"/> stays false,
    /// every read still returns what Redis holds, nothing is served from L1, and the library says why in its log.
    /// A hang or a coherent-but-deaf cache would be the failure this test exists to exclude.
    /// </summary>
    [Fact]
    public async Task Broadcast_ClientCertificateOnlyThroughTheEvent_IsNotSupported_FailsLoudly()
    {
        using var ca = ResilienceSupport.LoadPemCertificate(ResilienceSupport.CaCertPath());
        using var client = AuthSupport.ClientCertificate();
        var key = AuthSupport.BroadcastKey("mtls-bcast-event-only");

        EdgeCaseProvider? handle = null;
        var clock = System.Diagnostics.Stopwatch.StartNew();
        try
        {
            handle = await AuthSupport.BuildAsync(o =>
            {
                o.Configuration = ThroughEvents(ca, client);
                o.TrackingMode = TrackingMode.Broadcast;
                o.KeyPrefixes.Add(AuthSupport.BroadcastPrefix);
            }, awaitReady: false, captureLogs: true);

            var startupFailure = await Record.ExceptionAsync(() => handle.Cache.Ready).WaitAsync(TimeSpan.FromSeconds(20));
            _out.WriteLine($"broadcast, cert via event only: Ready settled after {clock.ElapsedMilliseconds} ms with " +
                           AuthSupport.Describe(startupFailure));
            Assert.NotNull(startupFailure);
            Assert.Empty(handle.Armer.RedirectTargets);

            // The startup exception itself names the cause: TLS authentication of the broadcast socket, not a timeout.
            // Asserted on the exception TYPE: which half of the limitation bites first is the platform's business.
            // The socket can read neither event, so OpenSSL (Linux) rejects the server certificate it was given no way
            // to validate ("...errors in the certificate chain: UntrustedRoot") before the client certificate comes
            // up at all, while macOS gets as far as the server aborting for want of one ("handshake failure").
            Assert.Contains("BCAST", AuthSupport.Describe(startupFailure), StringComparison.Ordinal);
            Assert.Contains(AuthSupport.Flatten(startupFailure!), e => e is System.Security.Authentication.AuthenticationException);

            await AuthSupport.AssertPassThroughStillCorrectAsync(
                handle.Cache, key, ExternalSet, "broadcast, client certificate via the event only");

            // The reason has to be in the log, not just in the absence of arming.
            // Matched on the message: the logger category is BroadcastTracker, so "broadcast" plus a level would match
            // any warning at all from the tracker.
            var explained = handle.LogLines.Where(l =>
                l.Contains("CLIENT TRACKING BCAST arm", StringComparison.Ordinal)
                && l.Contains("AuthenticationException", StringComparison.Ordinal))
                .ToArray();
            Assert.NotEmpty(explained);
            foreach (var line in explained.Take(3)) _out.WriteLine(line);
        }
        catch (TimeoutException)
        {
            Assert.Fail($"broadcast with the certificate only on the event hung for {clock.Elapsed.TotalSeconds:0.0} s.");
        }
        finally
        {
            if (handle is not null) await handle.DisposeAsync();
            AuthSupport.Mtls("DEL", key);
        }
    }

    // --- no client certificate ------------------------------------------------------------------------

    /// <summary>
    /// No client certificate at all, Redirect mode: the server demands one, so the TLS handshake fails and the
    /// failure must be prompt and unambiguous rather than a hang or a silent pass-through with no explanation.
    /// </summary>
    [Fact]
    public async Task NoClientCertificate_FailsLoudlyAndPromptly()
    {
        using var ca = ResilienceSupport.LoadPemCertificate(ResilienceSupport.CaCertPath());
        var (failure, elapsed) = await AuthSupport.FailsPromptlyAsync(async () =>
        {
            var handle = await EdgeCaseSupport.BuildAsync(o => o.Configuration = ThroughEvents(ca, null), awaitReady: false);
            try
            {
                await handle.Cache.Ready;
            }
            finally
            {
                await handle.DisposeAsync();
            }
        }, TimeSpan.FromSeconds(20), "mTLS with no client certificate");

        _out.WriteLine($"no client certificate: {elapsed.TotalMilliseconds:0} ms, {AuthSupport.Describe(failure)}");
        Assert.NotNull(failure);
        Assert.IsAssignableFrom<RedisConnectionException>(failure);
    }
}
