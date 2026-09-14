using RedisNearCache.Tests.EdgeCases;
using StackExchange.Redis;
using Xunit;
using Xunit.Abstractions;

namespace RedisNearCache.Tests.Resilience;

/// <summary>
/// TLS with a private CA, which is what almost every managed Redis actually looks like. The interesting part
/// is not that TLS works - StackExchange.Redis does that - but that RedisNearCache's private multiplexer is
/// built from a <em>clone</em> of the caller's <see cref="ConfigurationOptions"/>
/// (<c>RedisNearCacheConnection.BuildConfiguration</c>), and a clone that dropped the caller's
/// <see cref="ConfigurationOptions.CertificateValidation"/> handler would make the private connection fail the
/// handshake against a self-signed chain while the caller's own multiplexer connects fine. The container is
/// TLS-only and its certificate is signed by <c>certs/ca.crt</c>, which is trusted by nothing on the machine,
/// so the handler is the only thing that can make this connect.
/// </summary>
public class TlsConnectionTests
{
    private readonly ITestOutputHelper _out;

    public TlsConnectionTests(ITestOutputHelper output) => _out = output;

    [Fact]
    public async Task TlsConnectionHonoursCallersCertificateCallback()
    {
        using var ca = ResilienceSupport.LoadPemCertificate(ResilienceSupport.CaCertPath());
        _out.WriteLine($"CA: {ca.Subject} (from {ResilienceSupport.CaCertPath()})");

        var invocations = 0;
        var rejected = 0;

        // The handler validates the presented certificate against the test CA, which is what a caller with a
        // private CA has to do; the default machine trust store rejects this chain.
        var options = ResilienceSupport.TlsConfiguration(ca, accepted =>
        {
            Interlocked.Increment(ref invocations);
            if (!accepted) Interlocked.Increment(ref rejected);
        });

        var key = TestHelpers.Key("tls");
        EdgeCaseProvider? handle = null;
        try
        {
            handle = await EdgeCaseSupport.BuildAsync(o => o.Configuration = options);
            var cache = handle.Cache;

            await cache.Ready;
            Assert.True(handle.Multiplexer.IsConnected, "the private TLS multiplexer is not connected.");
            _out.WriteLine($"certificate callback invoked {Volatile.Read(ref invocations)} time(s), rejected {Volatile.Read(ref rejected)}");

            // The only multiplexer these options were ever given to is the private one, so a non-zero count
            // means BuildConfiguration's Clone carried the handler across.
            Assert.True(Volatile.Read(ref invocations) > 0,
                "the caller's CertificateValidation handler was never invoked: RedisNearCacheConnection.BuildConfiguration " +
                "did not carry it onto the private multiplexer.");
            Assert.Equal(0, Volatile.Read(ref rejected));

            // Tracking must be armed over TLS just like anywhere else.
            Assert.NotEmpty(handle.Armer.RedirectTargets);

            ResilienceSupport.Tls("SET", key, "v1");
            Assert.True(await TestHelpers.ReadUntilCachedAsync(cache, key, "v1"), "the key was not cached over TLS.");

            var hitsBefore = cache.Statistics.Hits;
            Assert.Equal("v1", await cache.GetAsync<string>(key));
            Assert.Equal(hitsBefore + 1, cache.Statistics.Hits);

            // A foreign write through the TLS container's own redis-cli must evict.
            ResilienceSupport.Tls("SET", key, "v2");
            var evicted = await Poll.UntilAsync(() => !cache.TryGetLocal<string>(key, out _), TimeSpan.FromSeconds(5));
            Assert.True(evicted, "invalidations are not delivered over a TLS connection.");
            Assert.Equal("v2", await cache.GetAsync<string>(key));
        }
        finally
        {
            if (handle is not null) await handle.DisposeAsync();
            try { ResilienceSupport.Tls("DEL", key); } catch (InvalidOperationException) { /* best effort */ }
        }
    }
}
