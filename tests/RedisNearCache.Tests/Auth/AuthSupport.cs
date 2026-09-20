using System.Globalization;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using RedisNearCache.Internal;
using RedisNearCache.Tests.EdgeCases;
using RedisNearCache.Tests.Sentinel;
using Xunit;

namespace RedisNearCache.Tests.Auth;

/// <summary>
/// Endpoints, credentials and redis-cli plumbing for the four containers <c>./auth-up.sh</c> creates, plus the one
/// end-to-end assertion nearly every test in this suite makes. Everything general is reused rather than rebuilt:
/// <see cref="RedisCli"/> (redis-cli inside a container), <see cref="Poll"/>, <see cref="TestHelpers"/> and
/// <see cref="EdgeCases.EdgeCaseSupport"/> (throwaway providers). What is added here is what none of those can
/// express: redis-cli invocations that carry credentials or TLS client certificates, reading
/// <c>ACL LOG</c> (the only authoritative answer to "did the server deny anything?"), and the shared
/// "the cache really works through this authentication configuration" assertion.
/// </summary>
internal static class AuthSupport
{
    // --- containers and endpoints ----------------------------------------------------------------------

    /// <summary>Standalone, <c>requirepass</c> on the default user plus the three ACL users.</summary>
    public const string AuthContainer = "redis-near-cache-auth";
    public const int AuthPort = 6450;
    public const string AuthEndPoint = "localhost:6450";

    /// <summary>Standalone, TLS-only, <c>--tls-auth-clients yes</c>, no password at all.</summary>
    public const string MtlsContainer = "redis-near-cache-mtls";
    public const int MtlsPort = 6460;

    /// <summary>The server certificate is <c>CN=localhost</c> with SANs <c>localhost</c> and <c>127.0.0.1</c>.</summary>
    public const string MtlsHost = "localhost";

    /// <summary>3 masters + 3 replicas in one container, password and ACL users on every node.</summary>
    public const string ClusterContainer = "redis-near-cache-auth-cluster";
    public static readonly int[] ClusterMasterPorts = [7300, 7301, 7302];
    public const string ClusterEndPoints = "127.0.0.1:7300,127.0.0.1:7301,127.0.0.1:7302";

    /// <summary>Data master 6470 (replicas 6471-6472) and three independent single-sentinel groups.</summary>
    public const string SentinelContainer = "redis-near-cache-auth-sentinel";
    public const int SentinelMasterPort = 6470;

    /// <summary>Sentinel <c>requirepass</c> is the same as the data nodes'.</summary>
    public const int SentinelSamePort = 26390;
    public const string SentinelSameService = "authsame";

    /// <summary>Sentinel has no <c>requirepass</c>; the data nodes still need one.</summary>
    public const int SentinelOpenPort = 26391;
    public const string SentinelOpenService = "authopen";

    /// <summary>Sentinel's <c>requirepass</c> differs from the data nodes'.</summary>
    public const int SentinelOtherPort = 26392;
    public const string SentinelOtherService = "authother";

    // --- credentials -----------------------------------------------------------------------------------

    /// <summary><c>requirepass</c> / <c>masterauth</c> on every auth-up.sh data node.</summary>
    public const string DefaultPassword = "rnc-default-pw";

    /// <summary><c>requirepass</c> of the <c>authother</c> sentinel only.</summary>
    public const string SentinelPassword = "rnc-sentinel-pw";

    public const string FullUser = "rnc-full";
    public const string FullPassword = "rnc-full-pw";

    public const string MinimalRedirectUser = "rnc-minimal-redirect";
    public const string MinimalBroadcastUser = "rnc-minimal-bcast";
    public const string MinimalPassword = "rnc-minimal-pw";

    // --- key prefixes ----------------------------------------------------------------------------------
    // The two minimal ACL users are restricted to `~t:*` and `~bc:*`, so every key a test touches has to start
    // with one of those. `t:in:` / `t:out:` sit inside `~t:*` but on opposite sides of KeyPrefixes, which is how
    // the OPTOUT + `CLIENT CACHING NO` transaction path is exercised under a restricted ACL.

    public const string InsidePrefix = "t:in:";
    public const string OutsidePrefix = "t:out:";
    public const string BroadcastPrefix = "bc:";

    public static string InsideKey(string suffix) => $"{InsidePrefix}{Guid.NewGuid():N}:{suffix}";

    public static string OutsideKey(string suffix) => $"{OutsidePrefix}{Guid.NewGuid():N}:{suffix}";

    public static string BroadcastKey(string suffix) => $"{BroadcastPrefix}{Guid.NewGuid():N}:{suffix}";

    // --- connection strings ----------------------------------------------------------------------------

    public static string PasswordOnly(string endPoints) => $"{endPoints},password={DefaultPassword}";

    public static string AsUser(string endPoints, string user, string password) =>
        $"{endPoints},user={user},password={password}";

    // --- redis-cli -------------------------------------------------------------------------------------

    private static string P(int port) => port.ToString(CultureInfo.InvariantCulture);

    /// <summary>redis-cli on the auth container as the <c>default</c> user (full rights).</summary>
    public static string Auth(params string[] args) =>
        RedisCli.Run(AuthContainer, ["-p", P(AuthPort), "-a", DefaultPassword, "--no-auth-warning", .. args]);

    /// <summary>redis-cli on one cluster node as <c>default</c>, following <c>MOVED</c> (<c>-c</c>).</summary>
    public static string Cluster(params string[] args) =>
        RedisCli.Run(ClusterContainer, ["-c", "-p", P(ClusterMasterPorts[0]), "-a", DefaultPassword, "--no-auth-warning", .. args]);

    /// <summary>redis-cli on one node of the auth cluster, without <c>-c</c> (so a wrong node answers MOVED).</summary>
    public static string ClusterNode(int port, params string[] args) =>
        RedisCli.Run(ClusterContainer, ["-p", P(port), "-a", DefaultPassword, "--no-auth-warning", .. args]);

    /// <summary>redis-cli on one data node of the auth-sentinel container as <c>default</c>.</summary>
    public static string SentinelData(int port, params string[] args) =>
        RedisCli.Run(SentinelContainer, ["-p", P(port), "-a", DefaultPassword, "--no-auth-warning", .. args]);

    /// <summary>redis-cli against one sentinel; <paramref name="password"/> is null for the passwordless one.</summary>
    public static string Sentinel(int port, string? password, params string[] args)
    {
        var argv = new List<string> { "-p", P(port) };
        if (password is not null)
        {
            argv.Add("-a");
            argv.Add(password);
            argv.Add("--no-auth-warning");
        }

        argv.AddRange(args);
        return RedisCli.Run(SentinelContainer, argv.ToArray());
    }

    /// <summary>redis-cli against the mTLS container, presenting the test client certificate.</summary>
    public static string Mtls(params string[] args) =>
        RedisCli.Run(MtlsContainer,
            ["--tls", "--cacert", "/certs/ca.crt", "--cert", "/certs/client.crt", "--key", "/certs/client.key",
             "-p", P(MtlsPort), .. args]);

    // --- certificates ----------------------------------------------------------------------------------

    /// <summary>
    /// The client certificate with its private key, loaded from the PKCS#12 file rather than from the PEM pair:
    /// on macOS a key imported from PEM is ephemeral and <see cref="System.Net.Security.SslStream"/> refuses to
    /// use it for client authentication, while the PFX works on both macOS and Linux.
    /// </summary>
    public static System.Security.Cryptography.X509Certificates.X509Certificate2 ClientCertificate() =>
        System.Security.Cryptography.X509Certificates.X509CertificateLoader.LoadPkcs12FromFile(
            Path.Combine(Resilience.ResilienceSupport.RepoRoot(), "certs", "client.pfx"), "rnc-test");

    // --- providers -------------------------------------------------------------------------------------

    /// <summary>
    /// <see cref="EdgeCases.EdgeCaseSupport.BuildAsync(Action{RedisNearCacheOptions}, bool)"/> with log capture:
    /// the <see cref="ConfigurationOptions"/>-taking overload there has no <c>captureLogs</c> switch, and the mTLS
    /// and Sentinel tests need both (a <see cref="ConfigurationOptions"/> object, because certificate callbacks
    /// cannot be expressed in a connection string, and the library's log lines, because what the library SAYS when
    /// it cannot arm is part of what "fails loudly" means).
    /// </summary>
    public static async Task<EdgeCaseProvider> BuildAsync(
        Action<RedisNearCacheOptions> configure,
        bool awaitReady = true,
        bool captureLogs = false)
    {
        var services = new ServiceCollection();
        var log = captureLogs ? new CapturingLoggerFactory() : null;
        if (log is not null) services.AddSingleton<ILoggerFactory>(log);
        services.AddRedisNearCache(configure);
        var provider = services.BuildServiceProvider();
        var handle = new EdgeCaseProvider(
            provider,
            provider.GetRequiredService<IRedisNearCache>(),
            provider.GetRequiredService<RedisNearCacheConnection>(),
            provider.GetRequiredService<ITrackingArmer>(),
            log);
        if (awaitReady) await handle.Cache.Ready;
        return handle;
    }

    // --- ACL LOG ---------------------------------------------------------------------------------------

    /// <summary>
    /// Empties the server's <c>ACL LOG</c>. Every test that claims "nothing was denied" resets it first and reads
    /// it again at the end: the library's own log lines only show denials it noticed, while <c>ACL LOG</c> also
    /// records the ones StackExchange.Redis swallowed on a background connection.
    /// </summary>
    /// <summary>
    /// Every connection of <paramref name="clientName"/> on the node (the private multiplexer's two, and the
    /// <c>-bcast</c> socket in Broadcast mode) is authenticated as <paramref name="user"/>. Without this a minimal-ACL
    /// test would pass just as well if <c>user=</c> were dropped somewhere and the connection fell back to the
    /// all-powerful default user: nothing would be denied then either.
    /// </summary>
    public static void AssertConnectedAs(Func<string[], string> cli, string clientName, string user, string context)
    {
        var ours = cli(["CLIENT", "LIST"]).Split('\n', StringSplitOptions.RemoveEmptyEntries)
            .Where(line => line.Contains($" name={clientName}", StringComparison.Ordinal)).ToArray();
        Assert.True(ours.Length > 0, $"{context}: no connection named {clientName}* on the node.");
        Assert.All(ours, line => Assert.Contains($" user={user} ", line + " ", StringComparison.Ordinal));
    }

    /// <summary><c>calls=N</c> of one command from <c>INFO commandstats</c>; 0 when the server has never run it.</summary>
    public static long CommandCalls(Func<string[], string> cli, string command)
    {
        var line = cli(["INFO", "commandstats"]).Split('\n')
            .FirstOrDefault(l => l.StartsWith($"cmdstat_{command}:", StringComparison.Ordinal));
        if (line is null) return 0;
        var calls = line[(line.IndexOf("calls=", StringComparison.Ordinal) + 6)..];
        return long.Parse(calls[..calls.IndexOf(',')]);
    }

    public static void ResetAclLog(Func<string[], string> cli) => cli(["ACL", "LOG", "RESET"]);

    /// <summary>Reads <c>ACL LOG</c>; the empty string means nothing has been denied since the last reset.</summary>
    public static string AclLog(Func<string[], string> cli) => cli(["ACL", "LOG"]).Trim();

    /// <summary>
    /// Fails with the raw <c>ACL LOG</c> when the server denied anything, and with the offending log lines when the
    /// library itself reported a permission problem. Both are needed: a denial on a StackExchange.Redis background
    /// connection never reaches the library's logger.
    /// </summary>
    public static void AssertNothingDenied(string aclLog, IReadOnlyCollection<string> logLines, string context)
    {
        Assert.True(aclLog.Length == 0, $"{context}: the server denied at least one command.\nACL LOG:\n{aclLog}");
        var complaints = logLines
            .Where(l => l.Contains("NOPERM", StringComparison.OrdinalIgnoreCase)
                        || l.Contains("no permissions", StringComparison.OrdinalIgnoreCase)
                        || l.Contains("WRONGPASS", StringComparison.OrdinalIgnoreCase))
            .ToArray();
        Assert.True(complaints.Length == 0,
            $"{context}: the library logged a permission problem.\n{string.Join("\n", complaints)}");
    }

    // --- the shared end-to-end assertion ---------------------------------------------------------------

    /// <summary>How long an invalidation may take to reach L1. Generous: docker exec alone costs ~80 ms.</summary>
    public static readonly TimeSpan EvictionDeadline = TimeSpan.FromSeconds(10);

    /// <summary>
    /// "The cache really works through this authentication configuration", in one place: startup finished, the
    /// facade is coherent, a key written through the cache reaches L1, a second read is served from L1 (counted as
    /// a hit), a write by a FOREIGN client (redis-cli as the <c>default</c> user, i.e. a different connection and a
    /// different Redis user) evicts it within the deadline, the next read returns the new value, and
    /// <see cref="IRedisNearCache.RemoveAsync"/> removes it. Leaves nothing behind on the server.
    /// </summary>
    /// <param name="externalSet">Writes a key from outside the cache; the container/credentials differ per test.</param>
    public static async Task AssertCacheWorksAsync(
        IRedisNearCache cache,
        string key,
        Action<string, string> externalSet,
        string context,
        TimeSpan? evictionDeadline = null)
    {
        Assert.True(cache.Ready.IsCompletedSuccessfully, $"{context}: Ready did not complete successfully.");
        Assert.True(cache.IsCoherent, $"{context}: the cache is not coherent, so it is serving nothing from L1.");

        await cache.SetAsync(key, "v1");
        Assert.True(await TestHelpers.ReadUntilCachedAsync(cache, key, "v1"), $"{context}: {key} never reached L1.");

        // Polled rather than asserted on a single read: in Broadcast mode the cache's own SetAsync echoes back as a
        // push (no NOLOOP on a connection that never reads), so the entry can be evicted once right after it lands.
        var hit = await Poll.UntilAsync(async () =>
        {
            var before = cache.Statistics.Hits;
            var value = await cache.GetAsync<string>(key);
            return value == "v1" && cache.Statistics.Hits == before + 1;
        }, TimeSpan.FromSeconds(10));
        Assert.True(hit, $"{context}: no read of {key} was served from L1. stats={cache.Statistics}");

        externalSet(key, "v2");
        var evicted = await Poll.UntilAsync(
            () => !cache.TryGetLocal<string>(key, out _), evictionDeadline ?? EvictionDeadline);
        Assert.True(evicted, $"{context}: an external write to {key} did not evict L1. stats={cache.Statistics}");
        Assert.Equal("v2", await cache.GetAsync<string>(key));

        Assert.True(await cache.RemoveAsync(key), $"{context}: RemoveAsync did not report a removal.");
        Assert.Null(await cache.GetAsync<string>(key));
    }

    /// <summary>
    /// The loud-failure counterpart of <see cref="AssertCacheWorksAsync"/> for a cache that could not arm: reads
    /// must still return what Redis holds, and nothing may be served from L1. This is the pass-through contract
    /// (<see cref="IRedisNearCache.IsCoherent"/> false), and the thing being excluded is a stale read.
    /// </summary>
    public static async Task AssertPassThroughStillCorrectAsync(
        IRedisNearCache cache,
        string key,
        Action<string, string> externalSet,
        string context)
    {
        Assert.False(cache.IsCoherent, $"{context}: the cache claims to be coherent.");

        externalSet(key, "p1");
        var hitsBefore = cache.Statistics.Hits;
        for (var i = 0; i < 3; i++)
        {
            Assert.Equal("p1", await cache.GetAsync<string>(key));
            Assert.False(cache.TryGetLocal<string>(key, out _), $"{context}: read {i + 1} populated L1 while not coherent.");
        }

        externalSet(key, "p2");
        Assert.Equal("p2", await cache.GetAsync<string>(key));
        Assert.Equal(hitsBefore, cache.Statistics.Hits);
    }

    /// <summary>
    /// Runs <paramref name="attempt"/> under a wall-clock deadline and returns what it threw. A hang is the failure
    /// mode being excluded, so hitting the deadline fails the test rather than surfacing as a timeout exception.
    /// </summary>
    public static async Task<(Exception? Failure, TimeSpan Elapsed)> FailsPromptlyAsync(
        Func<Task> attempt, TimeSpan deadline, string context)
    {
        var clock = System.Diagnostics.Stopwatch.StartNew();
        try
        {
            var failure = await Record.ExceptionAsync(attempt).WaitAsync(deadline);
            return (failure, clock.Elapsed);
        }
        catch (TimeoutException)
        {
            Assert.Fail($"{context}: hung for {clock.Elapsed.TotalSeconds:0.0} s instead of failing.");
            throw; // unreachable; Assert.Fail throws.
        }
    }

    /// <summary>Every message in an exception chain, <see cref="AggregateException.InnerExceptions"/> included.</summary>
    public static IEnumerable<Exception> Flatten(Exception ex)
    {
        yield return ex;
        if (ex is AggregateException aggregate)
        {
            foreach (var inner in aggregate.InnerExceptions)
            {
                foreach (var e in Flatten(inner)) yield return e;
            }
        }
        else if (ex.InnerException is { } single)
        {
            foreach (var e in Flatten(single)) yield return e;
        }
    }

    public static string Describe(Exception? ex) =>
        ex is null ? "<nothing>" : string.Join(" | ", Flatten(ex).Select(e => $"{e.GetType().Name}: {e.Message}"));
}
