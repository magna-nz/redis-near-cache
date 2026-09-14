using System.Diagnostics;
using System.Globalization;
using System.Net;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using StackExchange.Redis;

namespace RedisNearCache.Tests.Resilience;

/// <summary>
/// Helpers needed only by the resilience tests. Everything general already exists and is reused:
/// <see cref="RedisCli"/> (redis-cli inside a container), <see cref="Poll"/> (deadline polling),
/// <see cref="TestHelpers"/>, <see cref="Chaos.DockerExec"/> (arbitrary argv inside a container),
/// <see cref="Chaos.ChaosSupport"/> (quiesce / stale audit / reconnect retry), <see cref="Chaos.ForeignClient"/>
/// and <see cref="EdgeCases.EdgeCaseSupport"/> (throwaway providers).
/// What is added here is what none of those can express: docker commands that are not <c>docker exec</c>
/// (<c>docker restart</c>), the TLS container's redis-cli invocation, reading the test CA off disk, finding our
/// subscriber connections by the pub/sub flag, and parsing <c>CLUSTER NODES</c> for failover/reshard control.
/// </summary>
internal static class ResilienceSupport
{
    public const string StandaloneContainer = RedisCli.StandaloneContainer;
    public const string ReplicaContainer = EdgeCases.EdgeCaseSupport.ReplicaContainer;
    public const string TlsContainer = "redis-near-cache-tls";
    public const int TlsPort = 6390;

    /// <summary>The TLS container speaks TLS only, so a plain <c>localhost:6390</c> connection string is not enough.</summary>
    public const string TlsHost = "localhost";

    /// <summary>Path inside the TLS container where docker-compose mounts <c>certs/</c>.</summary>
    private const string ContainerCaPath = "/certs/ca.crt";

    // --- docker -----------------------------------------------------------------------------------------

    /// <summary>
    /// A raw <c>docker</c> invocation. <see cref="Chaos.DockerExec"/> always prefixes <c>exec &lt;container&gt;</c>,
    /// which cannot express <c>docker restart</c>; this is the same plumbing without that assumption.
    /// </summary>
    public static (int ExitCode, string StdOut, string StdErr) Docker(params string[] args)
    {
        var psi = new ProcessStartInfo("docker")
        {
            RedirectStandardOutput = true,
            RedirectStandardError = true,
        };
        foreach (var a in args) psi.ArgumentList.Add(a);

        using var p = Process.Start(psi)!;
        var stdout = p.StandardOutput.ReadToEnd();
        var stderr = p.StandardError.ReadToEnd();
        p.WaitForExit();
        return (p.ExitCode, stdout.Trim(), stderr.Trim());
    }

    /// <summary><see cref="Docker"/> without holding a thread-pool thread while docker runs (see <see cref="DockerProcess"/>).</summary>
    public static Task<(int ExitCode, string StdOut, string StdErr)> DockerAsync(params string[] args) =>
        DockerProcess.RunAsync(args);

    /// <summary>Stops and starts a container in place. Blocks until docker reports the restart done.</summary>
    public static void RestartContainer(string container)
    {
        var result = Docker("restart", "-t", "5", container);
        if (result.ExitCode != 0)
            throw new InvalidOperationException($"docker restart {container} exited {result.ExitCode}: {result.StdErr}");
    }

    /// <summary>
    /// redis-cli against the TLS-only container, trusting the test CA. Deliberately NOT overloaded with a
    /// password-taking variant: with <c>params string[]</c> both overloads would be applicable and
    /// <c>Tls("SET", k, v)</c> would silently bind the command name to the password parameter.
    /// </summary>
    public static string Tls(params string[] args) =>
        RedisCli.Run(TlsContainer, TlsCliArgs(null, args));

    /// <summary>Non-throwing form, for <c>finally</c> blocks that must restore server state either way.</summary>
    public static (int ExitCode, string StdOut, string StdErr) TryTls(string? password, params string[] args)
    {
        var argv = new List<string> { "redis-cli" };
        argv.AddRange(TlsCliArgs(password, args));
        return Chaos.DockerExec.Run(TlsContainer, argv.ToArray());
    }

    private static string[] TlsCliArgs(string? password, string[] args)
    {
        var full = new List<string> { "--tls", "--cacert", ContainerCaPath, "-p", TlsPort.ToString(CultureInfo.InvariantCulture) };
        if (password is not null)
        {
            full.Add("-a");
            full.Add(password);
            full.Add("--no-auth-warning");
        }

        full.AddRange(args);
        return full.ToArray();
    }

    /// <summary>
    /// <see cref="ConfigurationOptions"/> for the TLS container: the server certificate is signed by
    /// <c>certs/ca.crt</c>, which no machine trust store knows, so the caller-supplied
    /// <see cref="ConfigurationOptions.CertificateValidation"/> handler is the only thing that can let the
    /// handshake through. <paramref name="observe"/> is called once per handshake with the verdict.
    /// </summary>
    public static ConfigurationOptions TlsConfiguration(X509Certificate2 ca, Action<bool>? observe = null)
    {
        var cfg = new ConfigurationOptions
        {
            Ssl = true,
            SslHost = TlsHost,
            AbortOnConnectFail = true,
            ConnectTimeout = 15_000,
            ConnectRetry = 3,
        };
        cfg.EndPoints.Add(TlsHost, TlsPort);
        cfg.CertificateValidation += (object _, X509Certificate? presented, X509Chain? _, System.Net.Security.SslPolicyErrors errors) =>
        {
            var ok = false;
            if (presented is not null)
            {
                using var leaf = X509CertificateLoader.LoadCertificate(presented.GetRawCertData());
                // A self-signed chain is the only defect tolerated; a wrong host name or a missing certificate
                // must still fail, otherwise the handler would not be validating anything.
                ok = ChainsTo(ca, leaf)
                     && (errors & ~System.Net.Security.SslPolicyErrors.RemoteCertificateChainErrors) == System.Net.Security.SslPolicyErrors.None;
            }

            observe?.Invoke(ok);
            return ok;
        };

        return cfg;
    }

    /// <summary>PING that reports false instead of throwing, for polling a container back to life.</summary>
    public static bool PingOk(string container, params string[] prefixArgs)
    {
        var args = new List<string> { "redis-cli" };
        args.AddRange(prefixArgs);
        args.Add("PING");
        var r = Chaos.DockerExec.Run(container, args.ToArray());
        return r.ExitCode == 0 && r.StdOut.Contains("PONG", StringComparison.Ordinal);
    }

    // --- certificates -----------------------------------------------------------------------------------

    /// <summary>
    /// The repository root, found by walking up from the test assembly until <c>certs/ca.crt</c> is there.
    /// The certificates are gitignored and generated by <c>certs/gen.sh</c>, so there is nothing to copy to
    /// the output directory; the tests read them where <c>up.sh</c> put them.
    /// </summary>
    public static string RepoRoot()
    {
        var dir = new DirectoryInfo(AppContext.BaseDirectory);
        while (dir is not null)
        {
            if (File.Exists(Path.Combine(dir.FullName, "certs", "ca.crt"))) return dir.FullName;
            dir = dir.Parent;
        }

        throw new InvalidOperationException(
            $"could not find certs/ca.crt above {AppContext.BaseDirectory}; run ./up.sh (or certs/gen.sh) first.");
    }

    public static string CaCertPath() => Path.Combine(RepoRoot(), "certs", "ca.crt");

    /// <summary>
    /// Loads a PEM certificate without the obsolete <c>X509Certificate2(string)</c> constructor
    /// (<c>X509CertificateLoader</c> only accepts DER, so the PEM is unwrapped first).
    /// </summary>
    public static X509Certificate2 LoadPemCertificate(string path)
    {
        var pem = File.ReadAllText(path);
        var fields = PemEncoding.Find(pem);
        var der = Convert.FromBase64String(pem[fields.Base64Data]);
        return X509CertificateLoader.LoadCertificate(der);
    }

    /// <summary>True when <paramref name="presented"/> chains to <paramref name="ca"/> and nothing else.</summary>
    public static bool ChainsTo(X509Certificate2 ca, X509Certificate2 presented)
    {
        using var chain = new X509Chain();
        chain.ChainPolicy.RevocationMode = X509RevocationMode.NoCheck;
        chain.ChainPolicy.TrustMode = X509ChainTrustMode.CustomRootTrust;
        chain.ChainPolicy.CustomTrustStore.Add(ca);
        return chain.Build(presented);
    }

    // --- client list ------------------------------------------------------------------------------------

    /// <summary>
    /// Ids of the subscriber connections of <paramref name="clientName"/> in a raw <c>CLIENT LIST</c> reply:
    /// our client name plus the <c>P</c> (pub/sub) flag. <see cref="EdgeCases.EdgeCaseSupport.ClientIdsNamed"/>
    /// returns every connection of the client; the subscriber is the one whose kill loses invalidations.
    /// </summary>
    public static IReadOnlyList<long> SubscriberIdsNamed(string clientList, string clientName)
    {
        var ids = new List<long>();
        foreach (var line in clientList.Split('\n', StringSplitOptions.RemoveEmptyEntries))
        {
            var fields = line.Split(' ', StringSplitOptions.RemoveEmptyEntries);
            if (Field(fields, "name=") != clientName) continue;
            if (Field(fields, "flags=") is not { } flags || !flags.Contains('P', StringComparison.Ordinal)) continue;
            if (long.TryParse(Field(fields, "id="), NumberStyles.Integer, CultureInfo.InvariantCulture, out var id)) ids.Add(id);
        }

        return ids;
    }

    private static string? Field(string[] fields, string prefix)
    {
        foreach (var f in fields)
        {
            if (f.StartsWith(prefix, StringComparison.Ordinal)) return f[prefix.Length..];
        }

        return null;
    }

    // --- cluster ----------------------------------------------------------------------------------------

    public static int PortOf(EndPoint endPoint) => endPoint switch
    {
        IPEndPoint ip => ip.Port,
        DnsEndPoint dns => dns.Port,
        _ => throw new InvalidOperationException($"unexpected endpoint {endPoint}"),
    };

    /// <summary>
    /// Parses <c>CLUSTER NODES</c> from one node of the cluster container. Returns an empty list rather than
    /// throwing while a node is mid-failover and refusing commands, so it can be used inside a poll.
    /// </summary>
    public static IReadOnlyList<ClusterNodeInfo> ClusterNodes(int port = 7100)
    {
        var r = Chaos.DockerExec.Run(RedisCli.ClusterContainer, "redis-cli", "-p", port.ToString(CultureInfo.InvariantCulture), "CLUSTER", "NODES");
        if (r.ExitCode != 0) return [];

        var nodes = new List<ClusterNodeInfo>();
        foreach (var line in r.StdOut.Split('\n', StringSplitOptions.RemoveEmptyEntries))
        {
            var f = line.Trim().Split(' ', StringSplitOptions.RemoveEmptyEntries);
            if (f.Length < 8) continue;

            // <id> <ip:port@cport[,hostname]> <flags> <master-id|-> <ping> <pong> <epoch> <link-state> <slot>...
            var addr = f[1];
            var at = addr.IndexOf('@', StringComparison.Ordinal);
            if (at > 0) addr = addr[..at];
            var colon = addr.LastIndexOf(':');
            if (colon < 0 || !int.TryParse(addr.AsSpan(colon + 1), NumberStyles.Integer, CultureInfo.InvariantCulture, out var nodePort)) continue;

            var flags = f[2];
            var slots = new List<(int From, int To)>();
            for (var i = 8; i < f.Length; i++)
            {
                // Importing/migrating markers look like [0-<-<id>]; they are not ownership.
                if (f[i].StartsWith('[')) continue;
                var dash = f[i].IndexOf('-', StringComparison.Ordinal);
                if (dash < 0)
                {
                    if (int.TryParse(f[i], NumberStyles.Integer, CultureInfo.InvariantCulture, out var single)) slots.Add((single, single));
                }
                else if (int.TryParse(f[i].AsSpan(0, dash), NumberStyles.Integer, CultureInfo.InvariantCulture, out var from)
                         && int.TryParse(f[i].AsSpan(dash + 1), NumberStyles.Integer, CultureInfo.InvariantCulture, out var to))
                {
                    slots.Add((from, to));
                }
            }

            nodes.Add(new ClusterNodeInfo(
                f[0],
                nodePort,
                flags.Contains("master", StringComparison.Ordinal),
                f[3] == "-" ? null : f[3],
                slots));
        }

        return nodes;
    }

    public static ClusterNodeInfo? NodeOnPort(IReadOnlyList<ClusterNodeInfo> nodes, int port) =>
        nodes.FirstOrDefault(n => n.Port == port);

    public static bool ClusterStateOk(int port)
    {
        var r = Chaos.DockerExec.Run(RedisCli.ClusterContainer, "redis-cli", "-p", port.ToString(CultureInfo.InvariantCulture), "CLUSTER", "INFO");
        return r.ExitCode == 0 && r.StdOut.Contains("cluster_state:ok", StringComparison.Ordinal);
    }

    /// <summary>The layout <c>cluster-up.sh</c> creates and every other cluster test assumes.</summary>
    public static readonly (int Port, int From, int To)[] DefaultSlotLayout =
    [
        (7100, 0, 5460),
        (7101, 5461, 10922),
        (7102, 10923, 16383),
    ];

    public static bool IsDefaultLayout()
    {
        var nodes = ClusterNodes();
        if (nodes.Count == 0) return false;
        foreach (var (port, from, to) in DefaultSlotLayout)
        {
            var node = NodeOnPort(nodes, port);
            if (node is null || !node.IsMaster) return false;
            if (node.Slots.Count != 1 || node.Slots[0] != (from, to)) return false;
        }

        return ClusterStateOk(7100);
    }

    /// <summary>Human-readable slot ownership, for failure messages and diagnostics.</summary>
    public static string DescribeLayout()
    {
        var nodes = ClusterNodes();
        return string.Join("; ", nodes.OrderBy(n => n.Port).Select(n =>
            $"{n.Port}{(n.IsMaster ? "M" : "S")}[{string.Join(",", n.Slots.Select(s => s.From == s.To ? $"{s.From}" : $"{s.From}-{s.To}"))}]"));
    }

    /// <summary>
    /// <c>redis-cli --cluster reshard</c> inside the cluster container. Blocking and slow (seconds), so callers
    /// run it on a background task while the read/write load continues.
    /// </summary>
    public static (int ExitCode, string StdOut, string StdErr) Reshard(string fromId, string toId, int slots) =>
        Chaos.DockerExec.Run(
            RedisCli.ClusterContainer,
            "redis-cli", "--cluster", "reshard", "127.0.0.1:7100",
            "--cluster-from", fromId,
            "--cluster-to", toId,
            "--cluster-slots", slots.ToString(CultureInfo.InvariantCulture),
            "--cluster-yes");

    /// <summary><c>CLUSTER FAILOVER</c> must be sent to the replica that should be promoted.</summary>
    public static (int ExitCode, string StdOut, string StdErr) ClusterFailover(int replicaPort, string? mode = null)
    {
        var args = new List<string> { "redis-cli", "-p", replicaPort.ToString(CultureInfo.InvariantCulture), "CLUSTER", "FAILOVER" };
        if (mode is not null) args.Add(mode);
        return Chaos.DockerExec.Run(RedisCli.ClusterContainer, args.ToArray());
    }

    /// <summary>
    /// Fails the cluster back until 7100-7102 are the masters again, which every other cluster test assumes.
    /// Called from a <c>finally</c>, so it retries rather than throwing: a plain <c>CLUSTER FAILOVER</c> first
    /// (coordinated, no data loss), then <c>FORCE</c>, and only as a last resort <c>TAKEOVER</c>.
    /// </summary>
    public static async Task<bool> RestoreDefaultMastersAsync(TimeSpan? timeout = null)
    {
        var deadline = DateTime.UtcNow + (timeout ?? TimeSpan.FromSeconds(120));
        var attempt = 0;
        while (true)
        {
            var nodes = ClusterNodes();
            var demoted = DefaultSlotLayout
                .Select(l => l.Port)
                .Where(p => NodeOnPort(nodes, p) is { IsMaster: false })
                .ToArray();
            if (demoted.Length == 0 && nodes.Count > 0) return true;
            if (DateTime.UtcNow >= deadline) return false;

            // Escalate only after the polite form has had several goes; a failover needs the promoted replica
            // to be in sync, which takes a moment right after the previous one.
            var mode = attempt switch { < 4 => null, < 8 => "FORCE", _ => "TAKEOVER" };
            foreach (var port in demoted) ClusterFailover(port, mode);
            attempt++;
            await Task.Delay(TimeSpan.FromSeconds(1)).ConfigureAwait(false);
        }
    }
}

/// <summary>One line of <c>CLUSTER NODES</c>.</summary>
internal sealed record ClusterNodeInfo(
    string Id,
    int Port,
    bool IsMaster,
    string? MasterId,
    IReadOnlyList<(int From, int To)> Slots);
