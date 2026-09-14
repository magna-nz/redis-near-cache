using System.Collections.Concurrent;
using System.Diagnostics;
using System.Globalization;
using System.Net;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using RedisNearCache.Internal;
using RedisNearCache.Tests.Chaos;
using RedisNearCache.Tests.EdgeCases;
using RedisNearCache.Tests.Resilience;
using StackExchange.Redis;
using Xunit.Abstractions;

namespace RedisNearCache.Tests.Sentinel;

/// <summary>
/// Helpers for the Sentinel tests, which run against the single <c>redis-near-cache-sentinel</c> container that
/// <c>sentinel-up.sh</c> creates: data servers on 6400-6402 (6400 is the initial master) and three sentinels on
/// 26379-26381 monitoring <c>mymaster</c>. Everything announces 127.0.0.1, so what Sentinel reports is reachable
/// from the host. General plumbing is reused (<see cref="RedisCli"/>, <see cref="DockerExec"/>, <see cref="Poll"/>,
/// <see cref="EdgeCaseProvider"/>, <see cref="ResilienceSupport.PortOf"/>); what is added is sentinel/replication
/// introspection, killing and restarting one data server, and a provider whose library log lines are captured so
/// a failing failover test shows what the armer actually did.
/// </summary>
internal static class SentinelSupport
{
    public const string Container = "redis-near-cache-sentinel";
    public const string ServiceName = "mymaster";

    public static readonly int[] DataPorts = [6400, 6401, 6402];
    public static readonly int[] SentinelPorts = [26379, 26380, 26381];

    /// <summary>The sentinels, not the data servers: the multiplexer asks them where <c>mymaster</c> is.</summary>
    public const string ConnectionString = "127.0.0.1:26379,127.0.0.1:26380,127.0.0.1:26381,serviceName=mymaster";

    private static string P(int port) => port.ToString(CultureInfo.InvariantCulture);

    // --- redis-cli ------------------------------------------------------------------------------------------

    /// <summary>redis-cli against one port inside the sentinel container; throws on a non-zero exit.</summary>
    public static string Cli(int port, params string[] args)
    {
        var full = new List<string> { "-p", P(port) };
        full.AddRange(args);
        return RedisCli.Run(Container, full.ToArray());
    }

    /// <summary>Non-throwing form, for polls and for servers that may be down.</summary>
    public static (int ExitCode, string StdOut, string StdErr) TryCli(int port, params string[] args)
    {
        var full = new List<string> { "redis-cli", "-p", P(port) };
        full.AddRange(args);
        return DockerExec.Run(Container, full.ToArray());
    }

    public static bool ContainerRunning()
    {
        var r = ResilienceSupport.Docker("inspect", "-f", "{{.State.Running}}", Container);
        return r.ExitCode == 0 && r.StdOut.Trim() == "true";
    }

    public static bool IsAlive(int port)
    {
        var r = TryCli(port, "PING");
        return r.ExitCode == 0 && r.StdOut.Contains("PONG", StringComparison.Ordinal);
    }

    // --- sentinel / replication view ------------------------------------------------------------------------

    /// <summary>The master port one sentinel reports for <c>mymaster</c>, or null if it does not answer.</summary>
    public static int? ReportedMaster(int sentinelPort)
    {
        var r = TryCli(sentinelPort, "SENTINEL", "get-master-addr-by-name", ServiceName);
        if (r.ExitCode != 0) return null;
        var lines = r.StdOut.Split('\n', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
        return lines.Length == 2 && int.TryParse(lines[1], NumberStyles.Integer, CultureInfo.InvariantCulture, out var port) ? port : null;
    }

    /// <summary>The master port when all three sentinels report the same one; otherwise null.</summary>
    public static int? AgreedMaster()
    {
        int? agreed = null;
        foreach (var s in SentinelPorts)
        {
            if (ReportedMaster(s) is not { } port) return null;
            if (agreed is not null && agreed != port) return null;
            agreed = port;
        }

        return agreed;
    }

    /// <summary><c>INFO replication</c> of one data server as key/value pairs; empty when the server is down.</summary>
    public static Dictionary<string, string> Replication(int port)
    {
        var result = new Dictionary<string, string>(StringComparer.Ordinal);
        var r = TryCli(port, "INFO", "replication");
        if (r.ExitCode != 0) return result;
        foreach (var line in r.StdOut.Split('\n', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries))
        {
            var colon = line.IndexOf(':');
            if (colon > 0) result[line[..colon]] = line[(colon + 1)..];
        }

        return result;
    }

    public static bool IsMaster(int port) => Replication(port).TryGetValue("role", out var role) && role == "master";

    public static bool IsReplicaOf(int port, int masterPort)
    {
        var info = Replication(port);
        return info.TryGetValue("role", out var role) && role == "slave"
               && info.TryGetValue("master_port", out var mp) && mp == P(masterPort)
               && info.TryGetValue("master_link_status", out var link) && link == "up";
    }

    /// <summary>
    /// Flags of each entry in a flat <c>SENTINEL replicas|master</c> reply printed by redis-cli (one field per line,
    /// name then value). Only the <c>port</c> and <c>flags</c> fields are needed.
    /// </summary>
    private static List<(int Port, string Flags)> PortsAndFlags(string output)
    {
        var entries = new List<(int, string)>();
        var lines = output.Split('\n', StringSplitOptions.TrimEntries);
        int? port = null;
        for (var i = 0; i + 1 < lines.Length; i++)
        {
            if (lines[i] == "port" && int.TryParse(lines[i + 1], NumberStyles.Integer, CultureInfo.InvariantCulture, out var p)) port = p;
            if (lines[i] == "flags" && port is { } known)
            {
                entries.Add((known, lines[i + 1]));
                port = null;
            }
        }

        return entries;
    }

    /// <summary>
    /// True when every sentinel agrees on <paramref name="master"/>, sees it with plain <c>master</c> flags (no
    /// failover in progress, not down), sees both other data servers as healthy replicas, and can reach quorum.
    /// Anything less and a <c>SENTINEL FAILOVER</c> may be refused or pick nothing.
    /// </summary>
    public static bool SentinelsHealthy(int master)
    {
        foreach (var s in SentinelPorts)
        {
            if (ReportedMaster(s) != master) return false;

            var m = TryCli(s, "SENTINEL", "master", ServiceName);
            if (m.ExitCode != 0 || PortsAndFlags(m.StdOut) is not [var (mPort, mFlags)] || mPort != master || mFlags != "master") return false;

            var replicas = TryCli(s, "SENTINEL", "replicas", ServiceName);
            if (replicas.ExitCode != 0) return false;
            var seen = PortsAndFlags(replicas.StdOut);
            foreach (var port in DataPorts.Where(p => p != master))
            {
                if (!seen.Any(e => e.Port == port && e.Flags == "slave")) return false;
            }

            var quorum = TryCli(s, "SENTINEL", "ckquorum", ServiceName);
            if (quorum.ExitCode != 0 || !quorum.StdOut.StartsWith("OK", StringComparison.Ordinal)) return false;
        }

        return true;
    }

    public static string Describe()
    {
        var parts = new List<string>();
        foreach (var s in SentinelPorts) parts.Add($"sentinel {s} -> {ReportedMaster(s)?.ToString(CultureInfo.InvariantCulture) ?? "?"}");
        foreach (var p in DataPorts)
        {
            var info = Replication(p);
            if (info.Count == 0)
            {
                parts.Add($"{p}: down");
                continue;
            }

            info.TryGetValue("role", out var role);
            info.TryGetValue("master_port", out var mp);
            info.TryGetValue("master_link_status", out var link);
            parts.Add(role == "master" ? $"{p}: master" : $"{p}: replica of {mp} link={link}");
        }

        return string.Join("; ", parts);
    }

    // --- known state -----------------------------------------------------------------------------------------

    /// <summary>
    /// Brings the deployment to a known, failover-ready state and returns the current master port: restarts any
    /// data server that a previous test killed (as a replica of the agreed master), then waits until exactly one
    /// server is master, the other two replicate from it with the link up, and every sentinel is healthy (see
    /// <see cref="SentinelsHealthy"/>). Throws with a description when that cannot be reached, so a broken
    /// environment fails the test loudly instead of letting it pass vacuously.
    /// </summary>
    public static async Task<int> EnsureHealthyAsync(TimeSpan? timeout = null)
    {
        if (!ContainerRunning())
            throw new InvalidOperationException($"container {Container} is not running; start it with ./sentinel-up.sh");

        var deadline = DateTime.UtcNow + (timeout ?? TimeSpan.FromSeconds(120));
        while (true)
        {
            if (AgreedMaster() is { } master && IsMaster(master))
            {
                foreach (var port in DataPorts.Where(p => p != master && !IsAlive(p)))
                    RestartAsReplica(port, master);

                if (DataPorts.Where(p => p != master).All(p => IsReplicaOf(p, master)) && SentinelsHealthy(master))
                    return master;
            }

            if (DateTime.UtcNow >= deadline)
                throw new InvalidOperationException("the sentinel deployment did not reach a healthy state: " + Describe() +
                                                    "\n(re-create it with ./sentinel-up.sh)");
            await Task.Delay(TimeSpan.FromMilliseconds(500));
        }
    }

    /// <summary>
    /// Asks a sentinel to fail over, retrying while it answers <c>-INPROG</c>/<c>-NOGOODSLAVE</c> (a previous
    /// failover still settling). Returns the last reply.
    /// </summary>
    public static async Task<(bool Ok, string Reply)> SentinelFailoverAsync(TimeSpan? timeout = null)
    {
        var deadline = DateTime.UtcNow + (timeout ?? TimeSpan.FromSeconds(30));
        while (true)
        {
            var r = TryCli(SentinelPorts[0], "SENTINEL", "FAILOVER", ServiceName);
            var reply = (r.StdOut + " " + r.StdErr).Trim();
            if (r.ExitCode == 0 && reply.StartsWith("OK", StringComparison.Ordinal)) return (true, reply);
            if (DateTime.UtcNow >= deadline) return (false, reply);
            await Task.Delay(TimeSpan.FromMilliseconds(500));
        }
    }

    /// <summary>Waits until all sentinels agree on a master other than <paramref name="oldMaster"/>.</summary>
    public static async Task<int?> WaitForNewMasterAsync(int oldMaster, TimeSpan timeout)
    {
        int? found = null;
        await Poll.UntilAsync(() =>
        {
            found = AgreedMaster() is { } m && m != oldMaster ? m : null;
            return found is not null;
        }, timeout, TimeSpan.FromMilliseconds(200));
        return found;
    }

    // --- killing and restarting one data server --------------------------------------------------------------

    /// <summary>
    /// Hard failure: <c>kill -9</c> of the redis-server process listening on <paramref name="port"/> (pid read from
    /// <c>INFO server</c>, since the images ship no pgrep). No shutdown handshake, no final replication.
    /// </summary>
    public static int Kill(int port)
    {
        var info = Cli(port, "INFO", "server");
        var pidLine = info.Split('\n', StringSplitOptions.TrimEntries).First(l => l.StartsWith("process_id:", StringComparison.Ordinal));
        var pid = int.Parse(pidLine["process_id:".Length..], NumberStyles.Integer, CultureInfo.InvariantCulture);
        // kill is a shell builtin in these images (there is no /bin/kill), hence bash -c.
        var r = DockerExec.Run(Container, "bash", "-c", $"kill -9 {P(pid)}");
        if (r.ExitCode != 0) throw new InvalidOperationException($"kill -9 {pid} ({port}) exited {r.ExitCode}: {r.StdErr}");
        return pid;
    }

    /// <summary>
    /// Starts a killed data server again with the arguments <c>sentinel-up.sh</c> recorded for it, as a replica
    /// of <paramref name="masterPort"/> (the last <c>--replicaof</c> wins over the recorded one and over anything
    /// Sentinel's CONFIG REWRITE left in the config file).
    /// </summary>
    public static void RestartAsReplica(int port, int masterPort)
    {
        var script = $"eval redis-server /sentinel/redis-{P(port)}.conf $(cat /sentinel/redis-{P(port)}.args) --replicaof 127.0.0.1 {P(masterPort)}";
        var r = DockerExec.Run(Container, "bash", "-c", script);
        if (r.ExitCode != 0) throw new InvalidOperationException($"restarting {port} exited {r.ExitCode}: {r.StdOut} {r.StdErr}");
    }

    // --- client list -------------------------------------------------------------------------------------------

    /// <summary><c>CLIENT LIST</c> lines of one data server whose name is <paramref name="clientName"/>.</summary>
    public static IReadOnlyList<string> ClientLines(int port, string clientName) =>
        Cli(port, "CLIENT", "LIST")
            .Split('\n', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
            .Where(l => l.Contains($" name={clientName} ", StringComparison.Ordinal))
            .ToArray();

    public static string? Field(string clientLine, string name) =>
        clientLine.Split(' ', StringSplitOptions.RemoveEmptyEntries)
            .FirstOrDefault(f => f.StartsWith(name + "=", StringComparison.Ordinal))?[(name.Length + 1)..];

    // --- provider -----------------------------------------------------------------------------------------------

    /// <summary>
    /// A throwaway provider connected through the sentinels with the plain connection string (the normal public
    /// entry point, <c>AddRedisNearCache(connectionString)</c>). The only extra registration is an
    /// <see cref="ILoggerFactory"/> that records the library's log lines, plus observers on the armer's and the
    /// private multiplexer's events, so failures can print a timeline.
    /// </summary>
    public static async Task<SentinelProvider> BuildAsync()
    {
        var log = new CapturingLoggerFactory();
        var services = new ServiceCollection();
        services.AddSingleton<ILoggerFactory>(log);
        services.AddRedisNearCache(ConnectionString);
        var provider = services.BuildServiceProvider();
        try
        {
            var cache = provider.GetRequiredService<IRedisNearCache>();
            var connection = provider.GetRequiredService<RedisNearCacheConnection>();
            var armer = provider.GetRequiredService<ITrackingArmer>();

            armer.Armed += e => log.Add("test", $"event Armed {e.EndPoint} redirect={e.RedirectClientId} reason={e.Reason}");
            armer.TrackingLost += ep => log.Add("test", $"event TrackingLost {ep}");
            armer.EndpointRemoved += ep => log.Add("test", $"event EndpointRemoved {ep}");
            var mux = connection.Multiplexer;
            mux.ConnectionFailed += (_, e) => log.Add("test", $"mux ConnectionFailed {e.EndPoint} {e.ConnectionType} {e.FailureType}");
            mux.ConnectionRestored += (_, e) => log.Add("test", $"mux ConnectionRestored {e.EndPoint} {e.ConnectionType}");
            mux.ConfigurationChanged += (_, e) => log.Add("test", $"mux ConfigurationChanged {e.EndPoint}");
            mux.ConfigurationChangedBroadcast += (_, e) => log.Add("test", $"mux ConfigurationChangedBroadcast {e.EndPoint}");

            await cache.Ready;
            return new SentinelProvider(new EdgeCaseProvider(provider, cache, connection, armer), log);
        }
        catch
        {
            await provider.DisposeAsync();
            throw;
        }
    }

    /// <summary>Ports of the endpoints the armer currently holds a redirect id for.</summary>
    public static int[] ArmedPorts(ITrackingArmer armer) =>
        armer.RedirectTargets.Keys.Select(ResilienceSupport.PortOf).OrderBy(p => p).ToArray();

    public static long? RedirectFor(ITrackingArmer armer, int port) =>
        armer.RedirectTargets.FirstOrDefault(kv => ResilienceSupport.PortOf(kv.Key) == port) is { Key: not null } kv ? kv.Value : null;

    /// <summary>Waits for a replica to hold <paramref name="expected"/> for a key, so a failover cannot lose it.</summary>
    public static Task<bool> ReplicatedAsync(int replicaPort, string key, string expected) =>
        Poll.UntilAsync(() => TryCli(replicaPort, "GET", key).StdOut == expected, TimeSpan.FromSeconds(15), TimeSpan.FromMilliseconds(100));

    /// <summary>
    /// For failure messages: the value of each key on every data server, and our client's connections there
    /// (id, flags, redirect, last command).
    /// </summary>
    public static string DescribeNodes(string clientName, params string[] keys)
    {
        var sb = new System.Text.StringBuilder();
        foreach (var port in DataPorts)
        {
            if (!IsAlive(port))
            {
                sb.AppendLine($"  {port}: down");
                continue;
            }

            var values = string.Join(", ", keys.Select(k => $"{k[^14..]}={TryCli(port, "GET", k).StdOut}"));
            sb.AppendLine($"  {port} ({(IsMaster(port) ? "master" : "replica")}): {values}");
            foreach (var line in ClientLines(port, clientName))
                sb.AppendLine($"    id={Field(line, "id")} flags={Field(line, "flags")} redir={Field(line, "redir") ?? "n/a"} cmd={Field(line, "cmd")} age={Field(line, "age")}");
        }

        return sb.ToString();
    }

    /// <summary>
    /// Servers of the private multiplexer (endpoint, M/R, down), for diagnostics. Never throws.
    /// </summary>
    public static string DescribeMultiplexer(IConnectionMultiplexer mux)
    {
        try
        {
            return string.Join(",", mux.GetServers().Select(s =>
                $"{s.EndPoint}{(s.IsConnected ? "" : "(down)")}{(s.IsReplica ? "R" : "M")}"));
        }
        catch (Exception ex)
        {
            return "<" + ex.GetType().Name + ">";
        }
    }
}

internal sealed class SentinelProvider(EdgeCaseProvider handle, CapturingLoggerFactory log) : IAsyncDisposable
{
    public EdgeCaseProvider Handle { get; } = handle;
    public CapturingLoggerFactory Log { get; } = log;
    public IRedisNearCache Cache => Handle.Cache;
    public ITrackingArmer Armer => Handle.Armer;
    public RedisNearCacheConnection Connection => Handle.Connection;
    public IConnectionMultiplexer Multiplexer => Handle.Multiplexer;

    public void Dump(ITestOutputHelper output)
    {
        output.WriteLine("---- library log / events ----");
        foreach (var line in Log.Lines) output.WriteLine(line);
    }

    public ValueTask DisposeAsync() => Handle.DisposeAsync();
}

/// <summary>
/// Minimal <see cref="ILoggerFactory"/> that keeps every line in memory with a timestamp relative to creation.
/// Implemented against the abstractions only, so the test project needs no extra logging package.
/// </summary>
internal sealed class CapturingLoggerFactory : ILoggerFactory
{
    private readonly ConcurrentQueue<string> _lines = new();
    private readonly Stopwatch _clock = Stopwatch.StartNew();

    public IReadOnlyCollection<string> Lines => _lines.ToArray();

    public void Add(string category, string message) =>
        _lines.Enqueue($"{_clock.Elapsed.TotalSeconds,8:0.000}s [{category}] {message}");

    public ILogger CreateLogger(string categoryName) => new Logger(this, categoryName);

    public void AddProvider(ILoggerProvider provider)
    {
    }

    public void Dispose()
    {
    }

    private sealed class Logger(CapturingLoggerFactory owner, string category) : ILogger
    {
        private readonly string _short = category[(category.LastIndexOf('.') + 1)..];

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => logLevel >= LogLevel.Debug;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
        {
            var message = formatter(state, exception);
            if (exception is not null) message += $" ({exception.GetType().Name}: {exception.Message})";
            owner.Add($"{logLevel} {_short}", message);
        }
    }
}
