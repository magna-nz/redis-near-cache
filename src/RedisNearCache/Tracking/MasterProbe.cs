using System.Net;
using Microsoft.Extensions.Logging;
using StackExchange.Redis;

namespace RedisNearCache.Tracking;

/// <summary>
/// Asks the deployment whether an endpoint is still one of its masters, for a retry loop deciding between "keep
/// retrying" and "forget it". The pure rules live in <see cref="MasterRole"/>; this class supplies the I/O they
/// need (the multiplexer's view, <c>CLUSTER NODES</c> from a connected node, DNS). Shared by both tracking modes so
/// they retire endpoints by the same rule.
/// </summary>
internal sealed class MasterProbe
{
    private readonly IConnectionMultiplexer _multiplexer;
    private readonly ILogger _logger;

    /// <summary>Creates a probe over the private multiplexer.</summary>
    public MasterProbe(IConnectionMultiplexer multiplexer, ILogger logger)
    {
        _multiplexer = multiplexer;
        _logger = logger;
    }

    /// <summary>
    /// Whether <paramref name="endPoint"/> is still a master of the deployment, for a retry loop deciding between
    /// "keep retrying" and "forget it". Decides from the multiplexer's view when that is enough; for a disconnected
    /// cluster master it asks a connected node for <c>CLUSTER NODES</c> (masters first); when nothing can answer,
    /// or the servers cannot even be enumerated, it stays a master: never forget an endpoint on the strength of
    /// nothing.
    /// </summary>
    public async Task<bool> IsKnownMasterAsync(EndPoint endPoint, CancellationToken cancellationToken) =>
        (await ProbeAsync(endPoint, cancellationToken).ConfigureAwait(false)).IsKnownMaster;

    /// <summary>
    /// As <see cref="IsKnownMasterAsync"/>, and also returns the <c>CLUSTER NODES</c> view the answer was taken from
    /// (null when the multiplexer's own view decided, or nothing could answer), so a caller retiring the endpoint can
    /// check who serves its slots now.
    /// </summary>
    public async Task<MasterProbeResult> ProbeAsync(EndPoint endPoint, CancellationToken cancellationToken)
    {
        IServer[] servers;
        try
        {
            servers = _multiplexer.GetServers();
        }
        catch (Exception ex)
        {
            _logger.LogDebug(ex, "RedisNearCache could not enumerate servers to check whether {EndPoint} is still a master", endPoint);
            return new MasterProbeResult(true, null);
        }

        if (servers.Length == 0) return new MasterProbeResult(true, null);
        var local = MasterRole.FromMultiplexer(endPoint, MasterRole.ToViews(servers));
        if (local is { } decided) return new MasterProbeResult(decided, null);

        if (await ReadClusterNodesAsync(servers, endPoint, cancellationToken).ConfigureAwait(false) is ({ } server, { } nodes))
        {
            var known = MasterRole.FromClusterNodes(endPoint, nodes);
            if (!known && endPoint is DnsEndPoint dns)
            {
                // Names did not match; the host may still resolve to the address the cluster reports for a slot owner.
                try
                {
                    var addresses = await Dns.GetHostAddressesAsync(dns.Host, cancellationToken).ConfigureAwait(false);
                    known = MasterRole.FromClusterNodes(endPoint, nodes, addresses);
                }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                {
                    throw;
                }
                catch (Exception ex)
                {
                    _logger.LogDebug(ex, "RedisNearCache could not resolve {Host} while checking whether {EndPoint} still owns slots", dns.Host, endPoint);
                }
            }

            _logger.LogDebug("RedisNearCache CLUSTER NODES from {Server}: {EndPoint} {State}", server, endPoint,
                known ? "still serves slots as a master" : "serves no slots as a master");
            return new MasterProbeResult(known, nodes);
        }

        _logger.LogDebug("RedisNearCache could not read the cluster topology from any connected node; still treating {EndPoint} as a master", endPoint);
        return new MasterProbeResult(true, null);
    }

    /// <summary>
    /// The current <c>CLUSTER NODES</c> view from a connected node other than <paramref name="exclude"/>, masters
    /// first. <c>IsCluster</c> is false outside a cluster; <c>Nodes</c> is null there, and in a cluster when no
    /// node could answer.
    /// </summary>
    public async Task<(bool IsCluster, IReadOnlyList<ClusterNodeView>? Nodes)> ClusterNodesAsync(EndPoint exclude, CancellationToken cancellationToken)
    {
        IServer[] servers;
        try
        {
            servers = _multiplexer.GetServers();
        }
        catch (Exception ex)
        {
            _logger.LogDebug(ex, "RedisNearCache could not enumerate servers to read the cluster topology");
            return (true, null); // cannot tell: the caller must not treat this as "not a cluster"
        }

        if (!servers.Any(s => s.ServerType == ServerType.Cluster)) return (false, null);
        var view = await ReadClusterNodesAsync(servers, exclude, cancellationToken).ConfigureAwait(false);
        return (true, view?.Nodes);
    }

    private async Task<(EndPoint Server, List<ClusterNodeView> Nodes)?> ReadClusterNodesAsync(IServer[] servers, EndPoint exclude, CancellationToken cancellationToken)
    {
        foreach (var server in servers.Where(s => s.IsConnected && s.EndPoint is not null && !s.EndPoint.Equals(exclude)).OrderBy(s => s.IsReplica ? 1 : 0))
        {
            ClusterConfiguration? configuration;
            try
            {
                configuration = await server.ClusterNodesAsync().WaitAsync(cancellationToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception ex)
            {
                _logger.LogDebug(ex, "RedisNearCache could not read CLUSTER NODES from {Server}", server.EndPoint);
                continue;
            }

            if (configuration is null) continue;
            var nodes = configuration.Nodes
                .Select(n => new ClusterNodeView(n.EndPoint, n.Hostname, n.IsReplica, n.Slots.Count > 0))
                .ToList();
            return (server.EndPoint!, nodes);
        }

        return null;
    }
}

/// <summary>The answer of <see cref="MasterProbe.ProbeAsync"/>.</summary>
/// <param name="IsKnownMaster">Whether the endpoint is still a master of the deployment.</param>
/// <param name="ClusterNodes">The <c>CLUSTER NODES</c> view that decided it, or null when none was needed or available.</param>
internal readonly record struct MasterProbeResult(bool IsKnownMaster, IReadOnlyList<ClusterNodeView>? ClusterNodes);
