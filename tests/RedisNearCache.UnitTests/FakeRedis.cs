using System.Net;
using System.Reflection;
using RedisNearCache.Internal;
using StackExchange.Redis;

namespace RedisNearCache.UnitTests;

/// <summary>
/// Minimal in-memory stand-ins for the parts of StackExchange.Redis that <c>TrackingArmer</c> and the facade touch,
/// built with <see cref="DispatchProxy"/> so no mocking package is needed. Anything not handled throws, so a test
/// fails loudly if the code under test starts depending on something the fake does not model.
/// </summary>
public class FakeProxy : DispatchProxy
{
    internal Func<MethodInfo, object?[], object?> Handler { get; set; } = (m, _) => throw new NotSupportedException(m.Name);

    protected override object? Invoke(MethodInfo? targetMethod, object?[]? args) => Handler(targetMethod!, args ?? []);

    internal static T Create<T>(Func<MethodInfo, object?[], object?> handler) where T : class
    {
        var proxy = DispatchProxy.Create<T, FakeProxy>();
        ((FakeProxy)(object)proxy).Handler = handler;
        return proxy;
    }
}

/// <summary>One data server. Role and connection state are plain fields the test flips.</summary>
internal sealed class FakeServer
{
    private readonly FakeMultiplexer _owner;

    public FakeServer(FakeMultiplexer owner, EndPoint endPoint, bool isReplica, ServerType serverType, long subscriberId)
    {
        _owner = owner;
        EndPoint = endPoint;
        IsReplica = isReplica;
        ServerType = serverType;
        SubscriberId = subscriberId;
        Proxy = FakeProxy.Create<IServer>(Handle);
    }

    public EndPoint EndPoint { get; }
    public volatile bool IsConnected = true;
    public volatile bool IsReplica;
    public ServerType ServerType { get; }
    public long SubscriberId { get; }
    public IServer Proxy { get; }

    /// <summary>
    /// The first element of the <c>ROLE</c> reply. Null (the default) derives it from <see cref="IsReplica"/> at the
    /// time <c>ROLE</c> is answered, so flipping the role flag is reflected without also updating this property;
    /// set explicitly to make the fake disagree with <see cref="IsReplica"/> (e.g. a replica whose ROLE still says
    /// "master" because the promotion raced the pre-arm's ROLE call).
    /// </summary>
    public string? RoleAnswer { get; set; }

    /// <summary>
    /// Answers for successive <c>ROLE</c> calls, consumed in order; once empty, <see cref="RoleAnswer"/> applies.
    /// Lets a test make the pre-arm's first ROLE say "slave" and its second (the one issued right after
    /// CLIENT TRACKING ON) say "master", i.e. a promotion that raced the arm.
    /// </summary>
    public Queue<string> RoleAnswers { get; } = new();

    /// <summary>The link-state element of a replica's <c>ROLE</c> reply ("connected" unless a test says otherwise).</summary>
    public string LinkState { get; set; } = "connected";

    /// <summary>How many <c>ROLE</c> calls this server has answered.</summary>
    public int RoleCalls => Volatile.Read(ref _roleCalls);
    private int _roleCalls;

    /// <summary>
    /// Called with every <c>ExecuteAsync</c> command as space-separated text (e.g. "CLIENT TRACKING OFF") before it is
    /// answered; the reply is withheld until the returned task completes. Lets a test hold a sweep mid-way.
    /// </summary>
    public Func<string, Task>? CommandHook { get; set; }

    private object? Handle(MethodInfo method, object?[] args)
    {
        var result = HandleCore(method, args);
        if (method.Name == "ExecuteAsync" && CommandHook is { } hook && result is Task<RedisResult> reply)
        {
            var commandArgs = args.Length == 2 && args[1] is object[] rest ? rest : [];
            return WithheldAsync(hook(string.Join(' ', commandArgs.Prepend(args[0]))), reply);
        }
        return result;
    }

    private static async Task<RedisResult> WithheldAsync(Task hold, Task<RedisResult> reply)
    {
        await hold.ConfigureAwait(false);
        return await reply.ConfigureAwait(false);
    }

    private object? HandleCore(MethodInfo method, object?[] args)
    {
        switch (method.Name)
        {
            case "get_EndPoint": return EndPoint;
            case "get_IsConnected": return IsConnected;
            case "get_IsReplica":
            case "get_IsSlave": return IsReplica;
            case "get_ServerType": return ServerType;
            case "ToString": return EndPoint.ToString();
            case "GetHashCode": return EndPoint.GetHashCode();
            case "ClientListAsync":
                return Task.FromResult(new[] { FakeRedis.Client(SubscriberId, _owner.ClientName, ClientFlags.PubSubSubscriber) });
            case "ClusterNodesAsync":
                return Task.FromResult<ClusterConfiguration?>(null);
            case "ExecuteAsync" when args.Length == 2 && args[1] is object[] commandArgs:
                if (commandArgs.Length > 0 && Equals(commandArgs[0], "TRACKINGINFO"))
                {
                    return Task.FromResult(RedisResult.Create(
                    [
                        RedisResult.Create((RedisValue)"flags"), RedisResult.Create([RedisResult.Create((RedisValue)"on")]),
                        RedisResult.Create((RedisValue)"redirect"), RedisResult.Create((RedisValue)SubscriberId),
                    ]));
                }
                // The armer calls server.ExecuteAsync("ROLE") with no extra arguments, so the command name lands in
                // args[0] and commandArgs (args[1]) is an empty object[]; a command with a sub-command (e.g. CLIENT
                // TRACKINGINFO above) puts it in commandArgs[0] instead, so both spots are checked.
                if (Equals(args[0], "ROLE") || (commandArgs.Length > 0 && Equals(commandArgs[0], "ROLE")))
                {
                    Interlocked.Increment(ref _roleCalls);
                    string role;
                    lock (RoleAnswers) role = RoleAnswers.Count > 0 ? RoleAnswers.Dequeue() : RoleAnswer ?? (IsReplica ? "slave" : "master");
                    // A real ROLE reply on a replica is [role, master-host, master-port, link-state, offset], and
                    // TrackingArmer's pre-arm check requires link-state "connected" (TrackingArmer.IsReplicaRole,
                    // requireConnected: true) before it will touch the node at all. A master's ROLE reply has a
                    // different shape, but nothing reads past the first element for that case.
                    RedisResult[] items = string.Equals(role, "slave", StringComparison.Ordinal)
                        ? [
                            RedisResult.Create((RedisValue)role),
                            RedisResult.Create((RedisValue)"127.0.0.1"),
                            RedisResult.Create((RedisValue)0L),
                            RedisResult.Create((RedisValue)LinkState),
                            RedisResult.Create((RedisValue)0L),
                          ]
                        : [RedisResult.Create((RedisValue)role)];
                    return Task.FromResult(RedisResult.Create(items));
                }
                return Task.FromResult(RedisResult.Create((RedisValue)"OK"));
            default:
                throw new NotSupportedException($"IServer.{method.Name} is not modelled by the fake");
        }
    }
}

/// <summary>The private multiplexer: a fixed set of servers, raisable events, and a database whose GET returns "v1".</summary>
internal sealed class FakeMultiplexer
{
    private readonly List<FakeServer> _servers = new();
    private readonly object _events = new();
    private EventHandler<ConnectionFailedEventArgs>? _connectionFailed;
    private EventHandler<ConnectionFailedEventArgs>? _connectionRestored;
    private EventHandler<EndPointEventArgs>? _configurationChanged;
    private EventHandler<EndPointEventArgs>? _configurationChangedBroadcast;

    public FakeMultiplexer(string clientName)
    {
        ClientName = clientName;
        Proxy = FakeProxy.Create<IConnectionMultiplexer>(Handle);
        Database = FakeProxy.Create<IDatabase>(HandleDatabase);
    }

    public string ClientName { get; }
    public IConnectionMultiplexer Proxy { get; }
    private IDatabase Database { get; }
    public RedisValue StoredValue { get; set; } = "v1";

    public FakeServer Add(int port, bool isReplica, ServerType serverType = ServerType.Standalone)
    {
        var server = new FakeServer(this, new IPEndPoint(IPAddress.Loopback, port), isReplica, serverType, subscriberId: 1000 + port);
        _servers.Add(server);
        return server;
    }

    public void RaiseConnectionFailed(FakeServer server, ConnectionType type)
    {
        EventHandler<ConnectionFailedEventArgs>? handler;
        lock (_events) handler = _connectionFailed;
        handler?.Invoke(Proxy, new ConnectionFailedEventArgs(Proxy, server.EndPoint, type, ConnectionFailureType.SocketClosed, new InvalidOperationException("fake"), "fake"));
    }

    public void RaiseConfigurationChanged(FakeServer server)
    {
        EventHandler<EndPointEventArgs>? handler;
        lock (_events) handler = _configurationChanged;
        handler?.Invoke(Proxy, new EndPointEventArgs(Proxy, server.EndPoint));
    }

    private object? Handle(MethodInfo method, object?[] args)
    {
        switch (method.Name)
        {
            case "add_ConnectionFailed": lock (_events) _connectionFailed += (EventHandler<ConnectionFailedEventArgs>)args[0]!; return null;
            case "remove_ConnectionFailed": lock (_events) _connectionFailed -= (EventHandler<ConnectionFailedEventArgs>)args[0]!; return null;
            case "add_ConnectionRestored": lock (_events) _connectionRestored += (EventHandler<ConnectionFailedEventArgs>)args[0]!; return null;
            case "remove_ConnectionRestored": lock (_events) _connectionRestored -= (EventHandler<ConnectionFailedEventArgs>)args[0]!; return null;
            case "add_ConfigurationChanged": lock (_events) _configurationChanged += (EventHandler<EndPointEventArgs>)args[0]!; return null;
            case "remove_ConfigurationChanged": lock (_events) _configurationChanged -= (EventHandler<EndPointEventArgs>)args[0]!; return null;
            case "add_ConfigurationChangedBroadcast": lock (_events) _configurationChangedBroadcast += (EventHandler<EndPointEventArgs>)args[0]!; return null;
            case "remove_ConfigurationChangedBroadcast": lock (_events) _configurationChangedBroadcast -= (EventHandler<EndPointEventArgs>)args[0]!; return null;
            case "GetServers": return _servers.Select(s => s.Proxy).ToArray();
            case "GetServer" when args.Length == 2 && args[0] is EndPoint endPoint:
                return _servers.FirstOrDefault(s => s.EndPoint.Equals(endPoint))?.Proxy
                       ?? throw new ArgumentException($"{endPoint} is not a server of this multiplexer");
            case "GetDatabase": return Database;
            case "DisposeAsync": return ValueTask.CompletedTask;
            case "Dispose": return null;
            case "ToString": return "FakeMultiplexer";
            case "GetHashCode": return 0;
            default:
                throw new NotSupportedException($"IConnectionMultiplexer.{method.Name} is not modelled by the fake");
        }
    }

    /// <summary>Remaining TTL the fake reports for every key: -1 (no expiry) unless a test sets it.</summary>
    public long StoredTtlMilliseconds { get; set; } = -1;

    /// <summary>When set, every PTTL faults with this exception (a server that rejects the command, or a timeout).</summary>
    public Exception? PttlFailure { get; set; }

    /// <summary>How many PTTL commands the fake has answered (or faulted).</summary>
    public int PttlCalls => Volatile.Read(ref _pttlCalls);
    private int _pttlCalls;

    /// <summary>When set, the typed TTL (the facade's fallback when the raw PTTL fails) faults with this exception.</summary>
    public Exception? TypedTtlFailure { get; set; }

    /// <summary>How many typed TTL calls the fake has answered (or faulted).</summary>
    public int TypedTtlCalls => Volatile.Read(ref _typedTtlCalls);
    private int _typedTtlCalls;

    private object? HandleDatabase(MethodInfo method, object?[] args)
    {
        switch (method.Name)
        {
            case "StringGetAsync" when args.Length > 0 && args[0] is RedisKey:
                return Task.FromResult(StoredValue);
            case "ExecuteAsync" when args.Length == 2 && Equals(args[0], "PTTL"):
                Interlocked.Increment(ref _pttlCalls);
                return PttlFailure is { } failure
                    ? Task.FromException<RedisResult>(failure)
                    : Task.FromResult(RedisResult.Create((RedisValue)StoredTtlMilliseconds));
            // The typed TTL, which the facade falls back to when the raw PTTL fails: routed and redirected like any
            // other keyed command. Null means no expiry (and a key that is already gone), as StackExchange.Redis does.
            case "KeyTimeToLiveAsync" when args.Length > 0 && args[0] is RedisKey:
                Interlocked.Increment(ref _typedTtlCalls);
                if (TypedTtlFailure is { } typedFailure) return Task.FromException<TimeSpan?>(typedFailure);
                return Task.FromResult<TimeSpan?>(StoredTtlMilliseconds < 0 ? null : TimeSpan.FromMilliseconds(StoredTtlMilliseconds));
            default:
                throw new NotSupportedException($"IDatabase.{method.Name} is not modelled by the fake");
        }
    }
}

/// <summary>A listener that never delivers anything; these tests are about the armer's lifecycle events.</summary>
internal sealed class SilentListener : IInvalidationListener
{
    public event Action<string>? KeyInvalidated { add { } remove { } }
    public event Action? FlushAll { add { } remove { } }
    public Task StartAsync(CancellationToken cancellationToken) => Task.CompletedTask;
    public ValueTask DisposeAsync() => ValueTask.CompletedTask;
}

internal static class FakeRedis
{
    /// <summary><see cref="RedisNearCacheConnection"/> has only a private constructor (by design); tests reach it by reflection.</summary>
    public static RedisNearCacheConnection Connection(FakeMultiplexer mux)
    {
        var ctor = typeof(RedisNearCacheConnection).GetConstructor(
            BindingFlags.Instance | BindingFlags.NonPublic, [typeof(IConnectionMultiplexer), typeof(string)])
            ?? throw new InvalidOperationException("RedisNearCacheConnection(IConnectionMultiplexer, string) not found");
        return (RedisNearCacheConnection)ctor.Invoke([mux.Proxy, mux.ClientName]);
    }

    public static ClientInfo Client(long id, string name, ClientFlags flags)
    {
        var client = new ClientInfo();
        Set(client, nameof(ClientInfo.Id), id);
        Set(client, nameof(ClientInfo.Name), name);
        Set(client, nameof(ClientInfo.Flags), flags);
        return client;
    }

    private static void Set(object target, string property, object value) =>
        (typeof(ClientInfo).GetProperty(property)?.GetSetMethod(nonPublic: true)
         ?? throw new InvalidOperationException($"ClientInfo.{property} has no setter")).Invoke(target, [value]);
}
