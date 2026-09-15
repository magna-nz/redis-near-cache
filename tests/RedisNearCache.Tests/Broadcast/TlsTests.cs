using System.Net.Security;
using System.Security.Cryptography.X509Certificates;
using RedisNearCache.Tests.EdgeCases;
using RedisNearCache.Tests.Resilience;
using StackExchange.Redis;
using Xunit;

namespace RedisNearCache.Tests.Broadcast;

/// <summary>
/// Broadcast mode against the TLS-only container (<c>localhost:6390</c>). The broadcast socket cannot see
/// <see cref="ConfigurationOptions.CertificateValidation"/> (it is an event, unreadable from outside
/// StackExchange.Redis - see the remarks on <c>BroadcastTracker</c>), so trust in the test CA must be established
/// through <see cref="ConfigurationOptions.SslClientAuthenticationOptions"/> instead, unlike
/// <see cref="ResilienceSupport.TlsConfiguration"/> (used by the Redirect-mode resilience tests), which uses the
/// event.
/// </summary>
public class TlsTests
{
    [Fact]
    public async Task ArmsAndEvictsOverTls()
    {
        var ca = ResilienceSupport.LoadPemCertificate(ResilienceSupport.CaCertPath());
        var cfg = BuildTlsConfiguration(ca);

        var handle = await EdgeCaseSupport.BuildAsync(o =>
        {
            o.Configuration = cfg;
            o.TrackingMode = TrackingMode.Broadcast;
            o.KeyPrefixes.Add(BroadcastKey.Prefix);
        });
        try
        {
            Assert.NotEmpty(handle.Armer.RedirectTargets);

            var key = BroadcastKey.New("tls");
            await handle.Cache.SetAsync(key, "v1");
            Assert.True(await TestHelpers.ReadUntilCachedAsync(handle.Cache, key, "v1"), $"{key} was not cached over TLS.");

            ResilienceSupport.Tls("SET", key, "v2");

            var evicted = await Poll.UntilAsync(() => !handle.Cache.TryGetLocal<string>(key, out _), TimeSpan.FromSeconds(10));
            Assert.True(evicted, "an external write over TLS did not evict L1 within the deadline.");
        }
        finally
        {
            await handle.DisposeAsync();
        }
    }

    /// <summary>
    /// Same trust decision as <see cref="ResilienceSupport.TlsConfiguration"/> (chains to the test CA; a wrong
    /// host name or missing certificate still fails), expressed through
    /// <see cref="ConfigurationOptions.SslClientAuthenticationOptions"/> so the broadcast socket - which cannot
    /// read the <see cref="ConfigurationOptions.CertificateValidation"/> event - can validate it too.
    /// </summary>
    private static ConfigurationOptions BuildTlsConfiguration(X509Certificate2 ca)
    {
        var cfg = new ConfigurationOptions
        {
            Ssl = true,
            SslHost = ResilienceSupport.TlsHost,
            AbortOnConnectFail = true,
            ConnectTimeout = 15_000,
            ConnectRetry = 3,
        };
        cfg.EndPoints.Add(ResilienceSupport.TlsHost, ResilienceSupport.TlsPort);
        cfg.SslClientAuthenticationOptions = host => new SslClientAuthenticationOptions
        {
            TargetHost = host,
            RemoteCertificateValidationCallback = (_, presented, _, errors) =>
            {
                if (presented is null) return false;
                using var leaf = X509CertificateLoader.LoadCertificate(presented.GetRawCertData());
                return ResilienceSupport.ChainsTo(ca, leaf)
                       && (errors & ~SslPolicyErrors.RemoteCertificateChainErrors) == SslPolicyErrors.None;
            },
        };
        return cfg;
    }
}
