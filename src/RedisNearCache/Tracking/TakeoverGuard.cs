using System.Collections.Concurrent;
using System.Net;
using Microsoft.Extensions.Logging;
using StackExchange.Redis;

namespace RedisNearCache.Tracking;

/// <summary>
/// Decides when an endpoint that stopped being a master may be forgotten. Forgetting it (<c>EndpointRemoved</c>)
/// lets the facade serve from L1 again, so first every master that serves now - whoever took the endpoint's slots
/// over included - must be tracked: until then, writes there produce no invalidations. Shared by both tracking
/// modes, which differ only in what "tracked" means (see the constructor), so they retire endpoints by one rule.
/// </summary>
internal sealed class TakeoverGuard
{
    /// <summary>While a master that lost its slots waits for their new owners to be armed: how many rounds between forced reconfigures.</summary>
    private const int ReconfigureEveryRounds = 6;

    /// <summary>While a master that lost its slots waits for their new owners to be armed: the round from which the wait is logged as a warning.</summary>
    private const int WarnAfterRounds = 3;

    /// <summary>Pause between rounds of <see cref="RetireWhenArmed"/>, whose rounds can return at once when no node answers <c>CLUSTER NODES</c>.</summary>
    private static readonly TimeSpan RetireRoundDelay = TimeSpan.FromSeconds(1);

    /// <summary>How long one round waits for the takeover to be armed before giving up until the next.</summary>
    private static readonly TimeSpan WaitPerRound = TrackingRetry.SlowInterval;

    private readonly Func<IConnectionMultiplexer> _multiplexer;
    private readonly ILogger _logger;
    private readonly Func<List<EndPoint>> _connectedMasters;
    private readonly Func<IReadOnlyCollection<EndPoint>> _trackedEndPoints;
    private readonly Action<string> _reconcile;
    private MasterProbe? _probe;

    /// <summary>Endpoints with a <see cref="RetireWhenArmed"/> loop running; at most one per endpoint.</summary>
    private readonly ConcurrentDictionary<EndPoint, object> _retiring = new();

    /// <summary>Creates the guard.</summary>
    /// <param name="multiplexer">The private multiplexer, resolved when first needed.</param>
    /// <param name="logger">The owning tracker's logger.</param>
    /// <param name="connectedMasters">Every connected master, as the owning tracker enumerates them.</param>
    /// <param name="trackedEndPoints">Every endpoint on which tracking is currently on (never one announced lost).</param>
    /// <param name="reconcile">The owning tracker's reconcile, which arms masters it has not armed yet.</param>
    public TakeoverGuard(
        Func<IConnectionMultiplexer> multiplexer,
        ILogger logger,
        Func<List<EndPoint>> connectedMasters,
        Func<IReadOnlyCollection<EndPoint>> trackedEndPoints,
        Action<string> reconcile)
    {
        _multiplexer = multiplexer;
        _logger = logger;
        _connectedMasters = connectedMasters;
        _trackedEndPoints = trackedEndPoints;
        _reconcile = reconcile;
    }

    /// <summary>The shared master probe over the private multiplexer.</summary>
    public MasterProbe Probe => _probe ??= new MasterProbe(_multiplexer(), _logger);

    /// <summary>True while a <see cref="RetireWhenArmed"/> loop runs for the endpoint.</summary>
    public bool IsRetiring(EndPoint endPoint) => _retiring.ContainsKey(endPoint);

    /// <summary>
    /// Starts (at most one per endpoint) a loop that calls <paramref name="retire"/> once <see cref="TakeoverArmedAsync"/>
    /// succeeds, re-checking <paramref name="stillRetiring"/> before every round and right before retiring. The loop
    /// ends without retiring when <paramref name="stillRetiring"/> turns false, and then calls
    /// <paramref name="abandoned"/> so the owner can hand an endpoint it still waits on to its retry loop.
    /// </summary>
    public void RetireWhenArmed(EndPoint endPoint, string cause, Func<bool> stillRetiring, Action retire, Action abandoned, CancellationToken token)
    {
        var marker = new object();
        if (!_retiring.TryAdd(endPoint, marker)) return; // a loop is already running for this endpoint

        _logger.LogInformation("RedisNearCache saw a {Cause} after which {EndPoint} is no longer a master of this deployment; retiring it once its takeover is armed", cause, endPoint);
        _ = Task.Run(async () =>
        {
            var retired = false;
            try
            {
                var rounds = 0;
                while (!token.IsCancellationRequested)
                {
                    if (rounds > 0) await Task.Delay(RetireRoundDelay, token).ConfigureAwait(false);
                    if (!stillRetiring()) return;
                    if (!await TakeoverArmedAsync(endPoint, null, ++rounds, token).ConfigureAwait(false)) continue;
                    // The takeover wait can take a while: re-check that nothing changed the endpoint's fate meanwhile.
                    if (!stillRetiring()) return;
                    retire();
                    retired = true;
                    return;
                }
            }
            catch (OperationCanceledException)
            {
                // shutting down
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "RedisNearCache retirement of {EndPoint} stopped unexpectedly", endPoint);
            }
            finally
            {
                _retiring.TryRemove(KeyValuePair.Create(endPoint, marker));
                if (!retired && !token.IsCancellationRequested)
                {
                    try { abandoned(); }
                    catch (Exception ex) { _logger.LogWarning(ex, "RedisNearCache failed to hand {EndPoint} back after its retirement stopped", endPoint); }
                }
            }
        }, CancellationToken.None);
    }

    /// <summary>
    /// True once every master that serves now is tracked, so <paramref name="retiring"/> may be forgotten. The
    /// multiplexer only relearns a promoted node's role at its periodic check, which nothing bounds, so a full
    /// reconfigure is forced (on the first round, then every <see cref="ReconfigureEveryRounds"/> rounds) and the
    /// owner's reconcile runs to arm the new masters at once; this then waits up to <see cref="WaitPerRound"/> for
    /// those arms and otherwise returns false for the caller to try again later.
    /// </summary>
    /// <param name="retiring">The endpoint about to be forgotten.</param>
    /// <param name="clusterNodes">The <c>CLUSTER NODES</c> view that retired it, if the probe needed one.</param>
    /// <param name="round">1 on the first attempt for this endpoint, 2 on the next, and so on.</param>
    /// <param name="cancellationToken">Cancellation.</param>
    public async Task<bool> TakeoverArmedAsync(EndPoint retiring, IReadOnlyList<ClusterNodeView>? clusterNodes, int round, CancellationToken cancellationToken)
    {
        var probe = Probe;
        var resolved = new Dictionary<string, IPAddress[]>(StringComparer.OrdinalIgnoreCase);

        // The multiplexer's own view may have decided the retirement (the node came back as a replica), but in a
        // cluster only the slot map says who serves the slots now.
        var isCluster = true;
        if (clusterNodes is null) (isCluster, clusterNodes) = await probe.ClusterNodesAsync(retiring, cancellationToken).ConfigureAwait(false);
        var unarmed = isCluster && clusterNodes is null
            ? ["the cluster topology (no node answered CLUSTER NODES)"]
            : await UnarmedMastersAsync(retiring, clusterNodes, resolved, cancellationToken).ConfigureAwait(false);
        if (unarmed.Count == 0)
        {
            if (_logger.IsEnabled(LogLevel.Debug))
                _logger.LogDebug("RedisNearCache found the takeover of {EndPoint} armed; slot owners [{Owners}], all nodes [{Nodes}]", retiring,
                    clusterNodes is null ? "(not a cluster)" : string.Join(", ", clusterNodes.Where(n => !n.IsReplica && n.OwnsSlots).Select(n => n.EndPoint)),
                    clusterNodes is null ? "" : string.Join(", ", clusterNodes.Select(n => $"{n.EndPoint}{(n.IsReplica ? "S" : "M")}{(n.OwnsSlots ? "+" : "")}")));
            return true;
        }

        if (round == 1 || round % ReconfigureEveryRounds == 0)
        {
            try
            {
                await _multiplexer().ConfigureAsync().WaitAsync(cancellationToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception ex)
            {
                _logger.LogDebug(ex, "RedisNearCache could not reconfigure the private multiplexer after {EndPoint} lost its slots", retiring);
            }
        }

        _reconcile("master retirement");
        if (clusterNodes is not null || !isCluster)
        {
            var waited = System.Diagnostics.Stopwatch.StartNew();
            while (waited.Elapsed < WaitPerRound)
            {
                await Task.Delay(TimeSpan.FromMilliseconds(100), cancellationToken).ConfigureAwait(false);
                unarmed = await UnarmedMastersAsync(retiring, clusterNodes, resolved, cancellationToken).ConfigureAwait(false);
                if (unarmed.Count > 0) continue;
                if (!isCluster) return true;

                // The view above is up to a wait old: the retiring node may have taken its slots back meanwhile.
                var (_, fresh) = await probe.ClusterNodesAsync(retiring, cancellationToken).ConfigureAwait(false);
                if (fresh is null || MasterRole.FromClusterNodes(retiring, fresh))
                {
                    _logger.LogDebug("RedisNearCache keeps {EndPoint} lost: the cluster topology could not be re-read, or it serves slots again", retiring);
                    return false;
                }
                unarmed = await UnarmedMastersAsync(retiring, fresh, resolved, cancellationToken).ConfigureAwait(false);
                if (unarmed.Count == 0) return true;
                clusterNodes = fresh;
            }
        }

        _logger.Log(round >= WarnAfterRounds ? LogLevel.Warning : round == 1 ? LogLevel.Information : LogLevel.Debug,
            "RedisNearCache keeps {EndPoint} although it serves no slots: not armed yet [{Unarmed}] (round {Round}); the cache stays in pass-through or on its current tracking",
            retiring, string.Join(", ", unarmed), round);
        return false;
    }

    /// <summary>
    /// What still lacks tracking before <paramref name="retiring"/> may be forgotten: every connected master other
    /// than it and, in a cluster, every slot-owning master of <paramref name="clusterNodes"/> (matched to tracked
    /// endpoints as <see cref="MasterProbe"/> matches them; <paramref name="resolved"/> caches host lookups). Outside
    /// a cluster, having no other connected master at all also counts: the multiplexer may see the old master
    /// demoted before it sees the new one. In a cluster, so do slots that no master of the view serves: that is how a
    /// failed master shows up before its replacement is promoted. Empty when nothing is missing.
    /// </summary>
    private async Task<List<string>> UnarmedMastersAsync(EndPoint retiring, IReadOnlyList<ClusterNodeView>? clusterNodes, Dictionary<string, IPAddress[]> resolved, CancellationToken cancellationToken)
    {
        var tracked = _trackedEndPoints().Where(ep => !ep.Equals(retiring)).ToArray();
        var unarmed = new List<string>();
        var others = 0;
        foreach (var master in _connectedMasters())
        {
            if (master.Equals(retiring)) continue;
            others++;
            if (!tracked.Contains(master)) unarmed.Add(master.ToString() ?? "?");
        }

        if (clusterNodes is null)
        {
            if (others == 0) unarmed.Add("a master to take over (none connected)");
            return unarmed;
        }

        foreach (var dns in tracked.OfType<DnsEndPoint>())
        {
            if (resolved.ContainsKey(dns.Host)) continue;
            try
            {
                resolved[dns.Host] = await Dns.GetHostAddressesAsync(dns.Host, cancellationToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception ex)
            {
                _logger.LogDebug(ex, "RedisNearCache could not resolve {Host} while matching armed endpoints to slot owners", dns.Host);
                resolved[dns.Host] = [];
            }
        }

        if (!MasterRole.ServesAllSlots(clusterNodes))
        {
            var served = clusterNodes.Where(n => !n.IsReplica).Sum(n => (long)n.SlotCount);
            unarmed.Add($"slots without a live master ({served} of {MasterRole.ClusterSlots} served)");
        }

        foreach (var node in clusterNodes)
        {
            if (node.IsReplica || !node.OwnsSlots) continue;
            if (tracked.Any(ep => MasterRole.Matches(ep, node, ep is DnsEndPoint d ? resolved.GetValueOrDefault(d.Host) : null))) continue;
            var name = node.EndPoint?.ToString() ?? "(no address)";
            if (!unarmed.Contains(name)) unarmed.Add($"slot owner {name}");
        }

        return unarmed;
    }
}
