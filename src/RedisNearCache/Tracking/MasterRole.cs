using System.Net;
using System.Net.Sockets;
using StackExchange.Redis;

namespace RedisNearCache.Tracking;

/// <summary>One server of the private multiplexer, as <c>GetServers()</c> reported it at one moment.</summary>
internal readonly record struct ServerView(EndPoint EndPoint, bool IsConnected, bool IsReplica, ServerType ServerType);

/// <summary>One node of a <c>CLUSTER NODES</c> reply, as seen by a connected node.</summary>
/// <param name="EndPoint">The address the node reports (StackExchange.Redis always parses the IP here, never the hostname).</param>
/// <param name="Hostname">The hostname the node announces (<c>cluster-announce-hostname</c>), if any.</param>
/// <param name="IsReplica">Whether the node is a replica in this view.</param>
/// <param name="OwnsSlots">Whether the node serves at least one slot in this view.</param>
internal readonly record struct ClusterNodeView(EndPoint? EndPoint, string? Hostname, bool IsReplica, bool OwnsSlots);

/// <summary>
/// Decides whether an endpoint is still a master of the deployment, which is what keeps a lost endpoint worth
/// retrying (and the facade in pass-through) rather than forgotten. Pure functions over snapshots, so the rules
/// are unit-testable without a Redis server.
/// </summary>
/// <remarks>
/// The multiplexer never changes the role it last saw for a node it cannot reach, so "not flagged replica" is
/// not enough: a killed master would stay a master forever. A disconnected master stops counting once the
/// current topology has a different master covering it.
/// </remarks>
internal static class MasterRole
{
    /// <summary>
    /// Decides from the multiplexer's own view. Returns <see langword="null"/> when the endpoint is a disconnected
    /// cluster master: only the cluster's slot map can tell whether a replica has taken its slots over.
    /// </summary>
    /// <returns>
    /// <see langword="false"/> when the multiplexer no longer lists the endpoint or flags it as a replica, or when
    /// (outside a cluster) it is disconnected and another connected, non-replica data server exists;
    /// <see langword="true"/> when it is a connected master, or a disconnected master with no replacement.
    /// </returns>
    public static bool? FromMultiplexer(EndPoint endPoint, IReadOnlyCollection<ServerView> servers)
    {
        ServerView? found = null;
        foreach (var server in servers)
        {
            if (!server.EndPoint.Equals(endPoint)) continue;
            found = server;
            break;
        }

        if (found is not { } self) return false;
        if (self.IsReplica) return false;
        if (self.IsConnected) return true;

        if (self.ServerType == ServerType.Cluster || servers.Any(s => s.ServerType == ServerType.Cluster)) return null;

        foreach (var server in servers)
        {
            if (server.EndPoint.Equals(endPoint)) continue;
            if (server.IsConnected && !server.IsReplica && server.ServerType != ServerType.Sentinel) return false;
        }

        return true;
    }

    /// <summary>
    /// Decides from a connected node's <c>CLUSTER NODES</c> view: the endpoint is a master while it is listed as a
    /// master that still serves slots. A node that is down but still owns its slots (no failover yet) stays a
    /// master; one whose slots were taken over, or that the cluster no longer lists, does not.
    /// </summary>
    /// <param name="endPoint">The multiplexer's endpoint (an <see cref="IPEndPoint"/> or a <see cref="DnsEndPoint"/>).</param>
    /// <param name="nodes">The view, or <see langword="null"/> when no connected node could provide one.</param>
    /// <param name="resolvedAddresses">Addresses a <see cref="DnsEndPoint"/> host resolved to, if known.</param>
    /// <returns><see langword="true"/> (stay safe) when there is no view at all.</returns>
    public static bool FromClusterNodes(EndPoint endPoint, IReadOnlyCollection<ClusterNodeView>? nodes, IReadOnlyCollection<IPAddress>? resolvedAddresses = null)
    {
        if (nodes is null || nodes.Count == 0) return true;

        foreach (var node in nodes)
        {
            if (node.IsReplica || !node.OwnsSlots) continue;
            if (Matches(endPoint, node, resolvedAddresses)) return true;
        }

        return false;
    }

    /// <summary>
    /// True when a cluster node is the multiplexer's endpoint. Never plain <see cref="EndPoint"/> equality: against
    /// a hostname-announcing cluster the multiplexer holds <c>DnsEndPoint(localhost:7201)</c> while
    /// <c>CLUSTER NODES</c> yields <c>127.0.0.1:7201</c>. Ports must agree; hosts agree when the addresses are
    /// equal, the node's announced hostname is the endpoint's host, or the endpoint's host resolved to the node's address.
    /// </summary>
    public static bool Matches(EndPoint endPoint, ClusterNodeView node, IReadOnlyCollection<IPAddress>? resolvedAddresses = null)
    {
        if (node.EndPoint is null) return false;
        if (!TryHostAndPort(endPoint, out var host, out var port) || !TryHostAndPort(node.EndPoint, out var nodeHost, out var nodePort))
            return endPoint.Equals(node.EndPoint);
        if (port != nodePort) return false;

        if (SameHost(host, nodeHost)) return true;
        if (node.Hostname is { Length: > 0 } announced && string.Equals(announced, host, StringComparison.OrdinalIgnoreCase)) return true;

        if (resolvedAddresses is not null && IPAddress.TryParse(nodeHost, out var nodeAddress))
        {
            var normalized = Normalize(nodeAddress);
            foreach (var address in resolvedAddresses)
            {
                if (Normalize(address).Equals(normalized)) return true;
            }
        }

        return false;
    }

    private static bool TryHostAndPort(EndPoint endPoint, out string host, out int port)
    {
        switch (endPoint)
        {
            case IPEndPoint ip:
                host = Normalize(ip.Address).ToString();
                port = ip.Port;
                return true;
            case DnsEndPoint dns:
                host = dns.Host;
                port = dns.Port;
                return true;
            default:
                host = "";
                port = 0;
                return false;
        }
    }

    private static bool SameHost(string a, string b)
    {
        if (string.Equals(a, b, StringComparison.OrdinalIgnoreCase)) return true;
        return IPAddress.TryParse(a, out var x) && IPAddress.TryParse(b, out var y) && Normalize(x).Equals(Normalize(y));
    }

    private static IPAddress Normalize(IPAddress address) =>
        address.AddressFamily == AddressFamily.InterNetworkV6 && address.IsIPv4MappedToIPv6 ? address.MapToIPv4() : address;

    /// <summary>Snapshot of every server the multiplexer reports, for <see cref="FromMultiplexer"/>.</summary>
    public static List<ServerView> ToViews(IServer[] servers)
    {
        var views = new List<ServerView>(servers.Length);
        foreach (var server in servers)
        {
            if (server.EndPoint is { } endPoint) views.Add(new ServerView(endPoint, server.IsConnected, server.IsReplica, server.ServerType));
        }

        return views;
    }
}
