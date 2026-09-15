using System.Collections.Concurrent;
using System.Text;
using Microsoft.Extensions.Logging;

namespace RedisNearCache.Tracking.Broadcast;

/// <summary>
/// One RESP3 connection to a single Redis node, owned end to end by RedisNearCache: a stream (a socket, or a
/// <see cref="System.Net.Security.SslStream"/> over one), a writer that encodes commands as arrays of bulk
/// strings, and a single read loop.
/// </summary>
/// <remarks>
/// Replies are matched to commands by order, which is what RESP guarantees: every command appends a
/// <see cref="TaskCompletionSource{TResult}"/> to a FIFO under the write lock, so the queue is in the same order
/// as the bytes on the wire, and the read loop completes them in that order. Out-of-band push frames
/// (<c>&gt;</c>) never consume a queue entry; they go to the handler supplied at construction, on the read loop
/// itself, so invalidations stay in the order the server sent them.
/// This type exists because StackExchange.Redis 3.x consumes RESP3 <c>invalidate</c> pushes internally: the
/// broadcast socket has to be outside the multiplexer to see them.
/// </remarks>
internal sealed class Resp3Connection : IAsyncDisposable
{
    private readonly Stream _stream;
    private readonly IDisposable? _owned;
    private readonly Resp3Reader _reader;
    private readonly Action<Resp3Value> _onPush;
    private readonly ILogger _logger;
    private readonly ConcurrentQueue<TaskCompletionSource<Resp3Value>> _pending = new();
    private readonly SemaphoreSlim _writeGate = new(1, 1);
    private readonly TaskCompletionSource _completion = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private readonly CancellationTokenSource _shutdown = new();
    private int _disposed;
    private int _started;

    /// <summary>Creates a connection over an already connected (and, for TLS, already authenticated) stream.</summary>
    /// <param name="stream">The transport. Disposed with this connection.</param>
    /// <param name="name">A short name for log messages, typically the endpoint.</param>
    /// <param name="onPush">Called on the read loop for every push frame. Must not throw.</param>
    /// <param name="logger">Logger for read-loop diagnostics.</param>
    /// <param name="owned">An extra resource (the socket) to dispose with the connection.</param>
    public Resp3Connection(Stream stream, string name, Action<Resp3Value> onPush, ILogger logger, IDisposable? owned = null)
    {
        _stream = stream;
        _owned = owned;
        _reader = new Resp3Reader(stream);
        _onPush = onPush;
        _logger = logger;
        Name = name;
    }

    /// <summary>A short name for log messages, typically the endpoint this connection goes to.</summary>
    public string Name { get; }

    /// <summary>
    /// The <c>CLIENT ID</c> of this connection on the server. Set by the owner right after the handshake; it is
    /// what identifies the socket that <c>CLIENT TRACKING</c> was armed on.
    /// </summary>
    public long ClientId { get; set; }

    /// <summary>
    /// The credentials this connection authenticated with, as read from the configuration at handshake time (null
    /// password when the deployment needs no auth). The owner compares them with the configuration's current values to
    /// notice a rotation and updates them after re-authenticating in place. Held as plain strings for the socket's
    /// lifetime, as the multiplexer's own <c>ConfigurationOptions</c> holds them.
    /// </summary>
    internal BroadcastCredentials? Authenticated { get; set; }

    /// <summary>
    /// The credentials the server most recently rejected (or that could not be applied), so the same pair is not
    /// retried on every keepalive tick; cleared when a rotation succeeds. A failed <c>AUTH</c> leaves the connection
    /// authenticated as before, so a rejected rotation does not make the socket unusable.
    /// </summary>
    internal BroadcastCredentials? Rejected { get; set; }

    /// <summary>Completes (always successfully) when the read loop ends, whatever ended it. Never faults.</summary>
    public Task Completion => _completion.Task;

    /// <summary>True once the read loop has ended or the connection was disposed.</summary>
    public bool IsClosed => _completion.Task.IsCompleted || Volatile.Read(ref _disposed) == 1;

    /// <summary>Starts the read loop. Call once, after construction.</summary>
    public void Start()
    {
        if (Interlocked.Exchange(ref _started, 1) == 1) return;
        _ = Task.Run(ReadLoopAsync, CancellationToken.None);
    }

    /// <summary>
    /// Sends one command and waits for its reply. A server error reply is thrown as a
    /// <see cref="RedisNearCacheTrackingException"/> carrying the server's message.
    /// </summary>
    public async Task<Resp3Value> ExecuteAsync(CancellationToken cancellationToken, params string[] args)
    {
        ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) == 1, this);
        if (args.Length == 0) throw new ArgumentException("A command needs at least one argument.", nameof(args));

        var payload = Encode(args);
        var completion = new TaskCompletionSource<Resp3Value>(TaskCreationOptions.RunContinuationsAsynchronously);

        await _writeGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (IsClosed) throw new Resp3ProtocolException($"The RESP3 connection to {Name} is closed.");
            // Enqueued under the write lock so the queue order matches the order of the bytes on the wire.
            _pending.Enqueue(completion);
            await _stream.WriteAsync(payload, cancellationToken).ConfigureAwait(false);
            await _stream.FlushAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            // The command may or may not have reached the wire; either way this connection is no longer usable.
            completion.TrySetException(ex);
            Observe(completion.Task);
            FailPending(ex);
            throw;
        }
        finally
        {
            try { _writeGate.Release(); } catch (ObjectDisposedException) { /* disposed underneath us */ }
        }

        try
        {
            var reply = await completion.Task.WaitAsync(cancellationToken).ConfigureAwait(false);
            if (reply.IsError)
                throw new RedisNearCacheTrackingException($"{args[0]} on {Name} failed: {reply.Text}");
            return reply;
        }
        catch (OperationCanceledException)
        {
            // The reply may still arrive and complete the task nobody is waiting on any more.
            Observe(completion.Task);
            throw;
        }
    }

    /// <summary>Encodes a command as a RESP array of bulk strings.</summary>
    internal static byte[] Encode(params string[] args)
    {
        var builder = new StringBuilder();
        builder.Append('*').Append(args.Length).Append("\r\n");
        foreach (var arg in args)
        {
            builder.Append('$').Append(Encoding.UTF8.GetByteCount(arg)).Append("\r\n").Append(arg).Append("\r\n");
        }

        return Encoding.UTF8.GetBytes(builder.ToString());
    }

    private async Task ReadLoopAsync()
    {
        Exception? failure = null;
        try
        {
            var token = _shutdown.Token;
            while (!token.IsCancellationRequested)
            {
                var value = await _reader.ReadValueAsync(token).ConfigureAwait(false);
                if (value.Kind == Resp3Kind.Push)
                {
                    Dispatch(value);
                    continue;
                }

                if (_pending.TryDequeue(out var completion))
                {
                    completion.TrySetResult(value);
                }
                else
                {
                    _logger.LogDebug("RedisNearCache received an unsolicited RESP3 frame ({Frame}) from {Name}", value.ToString(), Name);
                }
            }
        }
        catch (OperationCanceledException)
        {
            // disposing
        }
        catch (Exception ex)
        {
            failure = ex;
            _logger.LogDebug(ex, "RedisNearCache RESP3 read loop for {Name} ended", Name);
        }
        finally
        {
            FailPending(failure ?? new Resp3ProtocolException($"The RESP3 connection to {Name} was closed."));
            _completion.TrySetResult();
        }
    }

    /// <summary>Hands a push frame to the owner. The read loop must survive anything the handler does.</summary>
    private void Dispatch(Resp3Value push)
    {
        try
        {
            _onPush(push);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "RedisNearCache failed to handle a RESP3 push frame from {Name}", Name);
        }
    }

    private void FailPending(Exception error)
    {
        while (_pending.TryDequeue(out var completion))
        {
            completion.TrySetException(error);
            Observe(completion.Task);
        }
    }

    /// <summary>Keeps a faulted reply that nobody awaits from surfacing as an unobserved task exception.</summary>
    private static void Observe(Task task) =>
        _ = task.ContinueWith(static t => _ = t.Exception, CancellationToken.None,
            TaskContinuationOptions.OnlyOnFaulted | TaskContinuationOptions.ExecuteSynchronously, TaskScheduler.Default);

    /// <summary>Closes the connection. Idempotent; never throws.</summary>
    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) == 1) return;

        try { await _shutdown.CancelAsync().ConfigureAwait(false); } catch (Exception ex) { _logger.LogDebug(ex, "RedisNearCache ignored an error cancelling the RESP3 read loop for {Name}", Name); }
        try { await _stream.DisposeAsync().ConfigureAwait(false); } catch (Exception ex) { _logger.LogDebug(ex, "RedisNearCache ignored an error closing the RESP3 stream to {Name}", Name); }
        try { _owned?.Dispose(); } catch (Exception ex) { _logger.LogDebug(ex, "RedisNearCache ignored an error closing the socket to {Name}", Name); }

        // The read loop only ends once the stream is gone, so wait for it after disposing the stream.
        if (Volatile.Read(ref _started) == 1)
        {
            try { await _completion.Task.WaitAsync(TimeSpan.FromSeconds(1)).ConfigureAwait(false); }
            catch (TimeoutException) { _logger.LogDebug("RedisNearCache RESP3 read loop for {Name} did not end within 1 s of disposal", Name); }
        }
        else
        {
            _completion.TrySetResult();
        }

        FailPending(new ObjectDisposedException(nameof(Resp3Connection)));
        _writeGate.Dispose();
        _shutdown.Dispose();
    }
}

/// <summary>
/// A user/password pair as sent to a broadcast socket, normalised to what goes on the wire: no user means Redis's
/// <c>default</c> user, and an empty password means no password. Record equality is what "rotated" means.
/// </summary>
internal sealed record BroadcastCredentials
{
    /// <summary>Normalises the configuration's raw values.</summary>
    public BroadcastCredentials(string? user, string? password)
    {
        User = user is { Length: > 0 } ? user : "default";
        Password = password is { Length: > 0 } ? password : null;
    }

    /// <summary>The user name as sent to the server.</summary>
    public string User { get; }

    /// <summary>The password, or null when the deployment needs no auth.</summary>
    public string? Password { get; }

    /// <summary>True when there is a password to authenticate with at all.</summary>
    public bool HasPassword => Password is not null;
}
