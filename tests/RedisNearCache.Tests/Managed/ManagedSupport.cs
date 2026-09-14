using StackExchange.Redis;

namespace RedisNearCache.Tests.Managed;

/// <summary>
/// Helpers shared by the managed-style emulation tests only. General plumbing (redis-cli via docker exec,
/// deadline polling, throwaway providers, endpoint-to-port, subscriber-id lookup) already exists and is
/// reused: <see cref="RedisCli"/>, <see cref="Poll"/>, <see cref="TestHelpers"/>,
/// <see cref="EdgeCases.EdgeCaseSupport"/> and <see cref="Resilience.ResilienceSupport"/>. What is added here
/// is what none of those can express: talking to the two containers <c>managed-up.sh</c> creates.
/// </summary>
internal static class ManagedSupport
{
    // --- restricted: ElastiCache-style disabled admin commands (managed-up.sh) --------------------------

    public const string RestrictedContainer = "redis-near-cache-restricted";
    public const int RestrictedPort = 6410;
    public const string RestrictedConnectionString = "localhost:6410";

    /// <summary>redis-cli inside the restricted container, which listens on <see cref="RestrictedPort"/>.</summary>
    public static string Restricted(params string[] args)
    {
        var full = new List<string> { "-p", RestrictedPort.ToString() };
        full.AddRange(args);
        return RedisCli.Run(RestrictedContainer, full.ToArray());
    }

    // --- cluster-hostname: ElastiCache/Azure OSS-cluster-style DNS endpoints (managed-up.sh) -------------

    public const string HostnameClusterContainer = "redis-near-cache-cluster-hostname";
    public static readonly int[] HostnameClusterMasterPorts = [7200, 7201, 7202];

    /// <summary>
    /// A single seed, given as an IP. The other two masters are then known to the multiplexer ONLY through
    /// the hostname the cluster announces (they surface as <c>DnsEndPoint localhost:720x</c>), which is what a
    /// managed configuration endpoint does. Seeding all three as <c>localhost:720x</c> would make every
    /// endpoint a hostname whatever the cluster announces, and prove nothing. With one seed the other masters
    /// are discovered asynchronously, so tests poll for them via <see cref="WaitForThreeArmedMastersAsync"/>.
    /// </summary>
    public const string HostnameClusterConnectionString = "127.0.0.1:7200";

    /// <summary>
    /// Waits until all three masters are connected and armed, and returns their endpoints. Unarmed masters are
    /// retried every 5 s, so the deadline covers several retries.
    /// </summary>
    public static async Task<System.Net.EndPoint[]> WaitForThreeArmedMastersAsync(EdgeCases.EdgeCaseProvider handle)
    {
        var ready = await Poll.UntilAsync(
            () => handle.Connection.ConnectedMasters().Count() == 3 && handle.Armer.RedirectTargets.Count == 3,
            TimeSpan.FromSeconds(20));
        var endpoints = handle.Connection.ConnectedMasters().Select(s => s.EndPoint!).ToArray();
        if (!ready)
            throw new Xunit.Sdk.XunitException(
                $"expected 3 connected, armed masters on the hostname-announcing cluster; connected: {string.Join(", ", endpoints)}; " +
                $"armed: {string.Join(", ", handle.Armer.RedirectTargets.Keys)}");
        return endpoints;
    }

    /// <summary>redis-cli in cluster mode (`-c`) against one port of the hostname-announcing cluster container.</summary>
    public static string HostnameCluster(int port, params string[] args)
    {
        var full = new List<string> { "-c", "-p", port.ToString() };
        full.AddRange(args);
        return RedisCli.Run(HostnameClusterContainer, full.ToArray());
    }

    /// <summary>
    /// True once <c>CLUSTER SHARDS</c> on the hostname-announcing cluster actually reports the announced
    /// hostname rather than an IP - the precondition every test in this file depends on, so a broken
    /// managed-up.sh flag set fails loudly instead of the tests passing vacuously.
    /// </summary>
    public static bool ReportsHostnameEndpoints() =>
        HostnameCluster(HostnameClusterMasterPorts[0], "CLUSTER", "SHARDS")
            .Contains("localhost", StringComparison.OrdinalIgnoreCase);

    /// <summary>
    /// Slot ranges per master port, parsed directly from <c>CLUSTER NODES</c> text. Deliberately NOT
    /// <see cref="TestHelpers.KeyForEndPoint"/>/<see cref="TestHelpers.KeyPerMaster"/>, which resolve a
    /// slot's owner through <c>IServer.ClusterNodes()?.GetBySlot(slot).EndPoint</c>: StackExchange.Redis's
    /// <c>ClusterConfiguration</c> always parses the bare <c>ip:port</c> out of <c>CLUSTER NODES</c>, never
    /// the hostname that <c>CLUSTER SLOTS</c>/<c>SHARDS</c> (and therefore
    /// <see cref="IConnectionMultiplexer.GetEndPoints"/>/<c>GetServers()</c>) hand back once
    /// <c>cluster-preferred-endpoint-type hostname</c> is set, so <c>.EndPoint</c> there never equals one of
    /// our <see cref="System.Net.DnsEndPoint"/> masters and those helpers find nothing. This is purely a
    /// limitation of that shared test helper's IP-endpoint assumption: RedisNearCache's own
    /// <see cref="RedisNearCache.Tracking.TrackingArmer"/> never calls <c>ClusterNodes()</c>/<c>GetBySlot</c>
    /// at all (it arms by <c>IConnectionMultiplexer.GetServers()</c> and finds its subscriber via
    /// <c>CLIENT LIST</c>), so the mismatch does not affect the library - only this test-only slot lookup.
    /// </summary>
    public static IReadOnlyDictionary<int, (int From, int To)[]> HostnameClusterSlotLayout()
    {
        var text = HostnameCluster(HostnameClusterMasterPorts[0], "CLUSTER", "NODES");
        var result = new Dictionary<int, List<(int From, int To)>>();
        foreach (var line in text.Split('\n', StringSplitOptions.RemoveEmptyEntries))
        {
            var fields = line.Trim().Split(' ', StringSplitOptions.RemoveEmptyEntries);
            if (fields.Length < 8) continue;
            if (!fields[2].Contains("master", StringComparison.Ordinal)) continue;

            var addr = fields[1];
            var at = addr.IndexOf('@', StringComparison.Ordinal);
            if (at > 0) addr = addr[..at];
            var colon = addr.LastIndexOf(':');
            if (colon < 0 || !int.TryParse(addr.AsSpan(colon + 1), out var port)) continue;

            var slots = new List<(int, int)>();
            for (var i = 8; i < fields.Length; i++)
            {
                if (fields[i].StartsWith('[')) continue;
                var dash = fields[i].IndexOf('-', StringComparison.Ordinal);
                if (dash < 0)
                {
                    if (int.TryParse(fields[i], out var single)) slots.Add((single, single));
                }
                else if (int.TryParse(fields[i].AsSpan(0, dash), out var from) && int.TryParse(fields[i].AsSpan(dash + 1), out var to))
                {
                    slots.Add((from, to));
                }
            }

            result[port] = slots;
        }

        return result.ToDictionary(kv => kv.Key, kv => kv.Value.ToArray());
    }

    /// <summary>Generates keys until one hashes into a slot range owned (per <c>CLUSTER NODES</c>) by <paramref name="port"/>.</summary>
    public static string KeyForPort(IConnectionMultiplexer mux, int port)
    {
        if (!HostnameClusterSlotLayout().TryGetValue(port, out var ranges))
            throw new InvalidOperationException($"port {port} is not a master in CLUSTER NODES.");

        for (var i = 0; i < 40_000; i++)
        {
            var candidate = TestHelpers.Key($"hostnamecluster{i}");
            var slot = mux.GetHashSlot(candidate);
            if (Array.Exists(ranges, r => slot >= r.From && slot <= r.To)) return candidate;
        }

        throw new InvalidOperationException($"could not find a key hashing to port {port}.");
    }

    /// <summary>One key per master port, keyed by port so callers can route writes with <see cref="HostnameCluster"/>.</summary>
    public static Dictionary<int, string> KeyPerMasterPort(IConnectionMultiplexer mux)
    {
        var layout = HostnameClusterSlotLayout();
        var result = new Dictionary<int, string>();
        for (var i = 0; i < 40_000 && result.Count < layout.Count; i++)
        {
            var candidate = TestHelpers.Key($"hostnamecluster{i}");
            var slot = mux.GetHashSlot(candidate);
            foreach (var (port, ranges) in layout)
            {
                if (result.ContainsKey(port)) continue;
                if (Array.Exists(ranges, r => slot >= r.From && slot <= r.To)) { result[port] = candidate; break; }
            }
        }

        return result;
    }
}
