using Xunit;
using Xunit.Abstractions;

namespace RedisNearCache.Tests.Compat;

/// <summary>
/// Diagnostics, not an assertion of behaviour: the CI matrix runs this whole suite against redis 6.2, 7.0,
/// 7.2, 7.4, 8 and valkey 8.1, and a green run says nothing about WHICH server produced it. This test writes
/// the server's own identification into the test output so every matrix leg's log names what it tested, and
/// so a skipped version-gated case elsewhere in this directory can be read against the version that caused it.
/// </summary>
public class ValkeyOrRedisVersionReportedTests
{
    private readonly ITestOutputHelper _out;

    public ValkeyOrRedisVersionReportedTests(ITestOutputHelper output) => _out = output;

    [Fact]
    public void ReportServerIdentification()
    {
        var info = CompatSupport.Info("server");

        var redisVersion = info.GetValueOrDefault("redis_version", "<absent>");
        var valkeyVersion = info.GetValueOrDefault("valkey_version", "<absent>");
        var mode = info.GetValueOrDefault("redis_mode", "<absent>");
        var os = info.GetValueOrDefault("os", "<absent>");

        _out.WriteLine($"standalone: redis_version={redisVersion} valkey_version={valkeyVersion} mode={mode} os={os}");
        _out.WriteLine($"parsed version used for feature gating: {CompatSupport.ServerVersion()}");

        var cluster = CompatSupport.ParseInfo(RedisCli.Cluster(ClusterCacheFixture.MasterPorts[0], "INFO", "server"));
        _out.WriteLine(
            $"cluster node {ClusterCacheFixture.MasterPorts[0]}: redis_version={cluster.GetValueOrDefault("redis_version", "<absent>")} " +
            $"valkey_version={cluster.GetValueOrDefault("valkey_version", "<absent>")} mode={cluster.GetValueOrDefault("redis_mode", "<absent>")}");

        // The only thing worth failing on: a server that identifies as neither.
        Assert.True(
            redisVersion != "<absent>" || valkeyVersion != "<absent>",
            "INFO server reported neither redis_version nor valkey_version.");
    }
}
