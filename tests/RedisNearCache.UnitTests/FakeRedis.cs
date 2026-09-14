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

    private object? Handle(MethodInfo method, object?[] args)
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

    private object? HandleDatabase(MethodInfo method, object?[] args) => method.Name switch
    {
        "StringGetAsync" when args.Length > 0 && args[0] is RedisKey => Task.FromResult(StoredValue),
        _ => throw new NotSupportedException($"IDatabase.{method.Name} is not modelled by the fake"),
    };
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
