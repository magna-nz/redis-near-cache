using System.Net.Security;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using System.Text.Json;
using StackExchange.Redis;

namespace RedisNearCache.Bench.Contenders;

/// <summary>
/// The one place every Redis connection of the benchmark is configured: source-of-truth connections, L2 caches,
/// backplanes, RedisNearCache's <see cref="RedisNearCacheOptions.Configuration"/>, the load test's writers and its admin
/// connection. Also the source-of-truth value codec (string as UTF-8, anything else as System.Text.Json UTF-8).
/// </summary>
public static class ContenderRedis
{
    /// <summary>
    /// Every key a contender keeps in Redis besides the source-of-truth keys lives under this prefix (L2 entries of the
    /// HybridCache, FusionCache and NearCacheHybridCache kinds), so a run can clear leftovers with one SCAN pattern.
    /// </summary>
    public const string OwnedKeyPrefix = "bench:l2:";

    /// <summary>
    /// Parses <see cref="ContenderSettings.Endpoint"/>. When it has <c>ssl=true</c> and
    /// <see cref="ContenderSettings.TlsCaCertificatePath"/> is set, installs a certificate validation callback that
    /// accepts a server certificate only if it chains to that CA and has no defect other than the (expected) untrusted
    /// root — the same rule the resilience tests use for the repo's TLS container.
    /// </summary>
    public static ConfigurationOptions BuildConfiguration(ContenderSettings settings, string? clientName = null, bool allowAdmin = false)
    {
        ArgumentNullException.ThrowIfNull(settings);
        var cfg = ConfigurationOptions.Parse(settings.Endpoint);
        if (clientName is not null) cfg.ClientName = clientName;
        if (allowAdmin) cfg.AllowAdmin = true;

        if (cfg.Ssl && settings.TlsCaCertificatePath is { Length: > 0 } caPath)
        {
            var ca = LoadPemCertificate(caPath); // kept alive by the closure for the lifetime of the configuration
            cfg.CertificateValidation += (object _, X509Certificate? presented, X509Chain? _, SslPolicyErrors errors) =>
            {
                if (presented is null) return false;
                using var leaf = X509CertificateLoader.LoadCertificate(presented.GetRawCertData());
                // A chain to the private CA is the only defect tolerated; a wrong host name must still fail.
                return ChainsTo(ca, leaf)
                       && (errors & ~SslPolicyErrors.RemoteCertificateChainErrors) == SslPolicyErrors.None;
            };
        }

        return cfg;
    }

    /// <summary>Connects a plain multiplexer configured by <see cref="BuildConfiguration"/>.</summary>
    public static async Task<ConnectionMultiplexer> ConnectAsync(ContenderSettings settings, string? clientName = null, bool allowAdmin = false) =>
        await ConnectionMultiplexer.ConnectAsync(BuildConfiguration(settings, clientName, allowAdmin)).ConfigureAwait(false);

    /// <summary>Source-of-truth encoding: <see cref="string"/> as UTF-8, anything else as System.Text.Json UTF-8.</summary>
    public static RedisValue Encode<T>(T value) => value switch
    {
        null => RedisValue.Null,
        string s => s,
        _ => JsonSerializer.SerializeToUtf8Bytes(value),
    };

    /// <summary>Inverse of <see cref="Encode{T}"/>; <c>default</c> for a missing key.</summary>
    public static T? Decode<T>(RedisValue value)
    {
        if (value.IsNull) return default;
        if (typeof(T) == typeof(string)) return (T)(object)(string)value!;
        return JsonSerializer.Deserialize<T>((byte[])value!);
    }

    private static X509Certificate2 LoadPemCertificate(string path)
    {
        var pem = File.ReadAllText(path);
        var fields = PemEncoding.Find(pem);
        var der = Convert.FromBase64String(pem[fields.Base64Data]);
        return X509CertificateLoader.LoadCertificate(der);
    }

    private static bool ChainsTo(X509Certificate2 ca, X509Certificate2 presented)
    {
        using var chain = new X509Chain();
        chain.ChainPolicy.RevocationMode = X509RevocationMode.NoCheck;
        chain.ChainPolicy.TrustMode = X509ChainTrustMode.CustomRootTrust;
        chain.ChainPolicy.CustomTrustStore.Add(ca);
        return chain.Build(presented);
    }
}
