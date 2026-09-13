using System.Net;
using System.Text.RegularExpressions;
using StackExchange.Redis;

namespace RedisNearCache.Tests;

internal static class TestHelpers
{
    /// <summary>A unique key prefix per test so tests can run against a shared database without colliding.</summary>
    public static string Key(string suffix) => $"t:{Guid.NewGuid():N}:{suffix}";

    /// <summary>Parses `calls=N` out of `cmdstat_&lt;command&gt;:calls=N,...` in the `commandstats` INFO section.</summary>
    public static long CommandCalls(IServer server, string command)
    {
        var sections = server.Info("commandstats");
        var key = $"cmdstat_{command}";
        foreach (var section in sections)
        {
            foreach (var entry in section)
            {
                if (entry.Key != key) continue;
                var m = Regex.Match(entry.Value, @"calls=(\d+)");
                if (m.Success) return long.Parse(m.Groups[1].Value);
            }
        }
        return 0;
    }

    /// <summary>Generates keys until one hashes to the given cluster node's slot range.</summary>
    public static string KeyForEndPoint(IConnectionMultiplexer mux, IServer anyServer, EndPoint endpoint)
    {
        for (var i = 0; i < 20_000; i++)
        {
            var candidate = Key($"node{i}");
            var slot = mux.GetHashSlot(candidate);
            var node = anyServer.ClusterNodes()?.GetBySlot(slot);
            if (node is not null && node.EndPoint?.Equals(endpoint) == true) return candidate;
        }
        throw new InvalidOperationException($"could not find a key hashing to node {endpoint}.");
    }

    /// <summary>Generates keys until one is found per distinct master endpoint.</summary>
    public static Dictionary<EndPoint, string> KeyPerMaster(IConnectionMultiplexer mux, IServer anyServer, IReadOnlyCollection<EndPoint> endpoints)
    {
        var result = new Dictionary<EndPoint, string>();
        for (var i = 0; i < 40_000 && result.Count < endpoints.Count; i++)
        {
            var candidate = Key($"node{i}");
            var slot = mux.GetHashSlot(candidate);
            var node = anyServer.ClusterNodes()?.GetBySlot(slot);
            if (node is null) continue;
            var match = endpoints.FirstOrDefault(e => e.Equals(node.EndPoint));
            if (match is not null && !result.ContainsKey(match)) result[match] = candidate;
        }
        return result;
    }
}
