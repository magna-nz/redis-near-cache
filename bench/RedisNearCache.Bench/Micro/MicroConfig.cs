using RedisNearCache.Bench.Contenders;

namespace RedisNearCache.Bench.Micro;

/// <summary>
/// Endpoint/TLS configuration shared by the BenchmarkDotNet and Sailfish micro suites, read from environment
/// variables so BenchmarkDotNet's out-of-process child processes (which do not see the parent's argv) still learn
/// where Redis is: <see cref="Program"/> sets <see cref="Payloads.EnvEndpoint"/> / <see cref="Payloads.EnvTlsCa"/>
/// from the command line before handing off to either tool, and both the parent and the children read them back
/// here. Sailfish runs in-process, so this is only strictly necessary for BenchmarkDotNet, but using the same
/// mechanism for both keeps one code path.
/// </summary>
public static class MicroConfig
{
    public const string EnvQuick = "RNC_BENCH_QUICK";

    /// <summary>Whether <c>--quick</c> was passed to the parent process. Read from the environment (not argv) so
    /// BenchmarkDotNet's out-of-process child processes, which only inherit the environment, see it too.</summary>
    public static bool Quick => Environment.GetEnvironmentVariable(EnvQuick) == "1";

    public static ContenderSettings BuildSettings(string instanceName, TimeSpan ttl)
    {
        var endpoint = Environment.GetEnvironmentVariable(Payloads.EnvEndpoint) is { Length: > 0 } e ? e : "localhost:6379";
        var tlsCa = Environment.GetEnvironmentVariable(Payloads.EnvTlsCa) is { Length: > 0 } ca ? ca : null;
        return new ContenderSettings
        {
            Endpoint = endpoint,
            TlsCaCertificatePath = tlsCa,
            Ttl = ttl,
            LocalSizeLimit = 100_000,
            InstanceName = instanceName,
        };
    }
}
