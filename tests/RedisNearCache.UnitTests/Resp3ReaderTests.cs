using System.Text;
using System.Threading.Channels;
using Microsoft.Extensions.Logging.Abstractions;
using RedisNearCache.Tracking.Broadcast;
using Xunit;

namespace RedisNearCache.UnitTests;

/// <summary>
/// The RESP3 decoder used by the broadcast (Redis Enterprise) tracking mode, driven from memory: no socket, no
/// Redis. Every case is also run with the bytes delivered one at a time and at random boundaries, because on a
/// real socket a frame arrives in whatever pieces the network gives it.
/// </summary>
public class Resp3ReaderTests
{
    private const string PushOneKey = ">2\r\n$10\r\ninvalidate\r\n*1\r\n$4\r\nnc:x\r\n";
    private const string PushNullKeys = ">2\r\n$10\r\ninvalidate\r\n_\r\n";

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(3)]
    [InlineData(7)]
    public async Task SimpleStringErrorAndInteger(int chunk)
    {
        var ok = await ParseAsync("+OK\r\n", chunk);
        Assert.Equal(Resp3Kind.SimpleString, ok.Kind);
        Assert.Equal("OK", ok.Text);

        var error = await ParseAsync("-ERR unknown subcommand\r\n", chunk);
        Assert.Equal(Resp3Kind.Error, error.Kind);
        Assert.True(error.IsError);
        Assert.Equal("ERR unknown subcommand", error.Text);

        var blobError = await ParseAsync("!21\r\nSYNTAX invalid syntax\r\n", chunk);
        Assert.Equal(Resp3Kind.Error, blobError.Kind);
        Assert.Equal("SYNTAX invalid syntax", blobError.Text);

        Assert.Equal(1234, (await ParseAsync(":1234\r\n", chunk)).Integer);
        Assert.Equal(-9, (await ParseAsync(":-9\r\n", chunk)).Integer);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(3)]
    [InlineData(7)]
    public async Task BulkStringsIncludingEmptyAndNull(int chunk)
    {
        var hello = await ParseAsync("$5\r\nhello\r\n", chunk);
        Assert.Equal(Resp3Kind.BulkString, hello.Kind);
        Assert.Equal("hello", hello.Text);

        var empty = await ParseAsync("$0\r\n\r\n", chunk);
        Assert.Equal(Resp3Kind.BulkString, empty.Kind);
        Assert.Equal(string.Empty, empty.Text);

        Assert.True((await ParseAsync("$-1\r\n", chunk)).IsNull);
        Assert.True((await ParseAsync("_\r\n", chunk)).IsNull);
        Assert.True((await ParseAsync("*-1\r\n", chunk)).IsNull);

        // Multi-byte UTF-8 payloads keep their length in bytes, not characters.
        var unicode = await ParseAsync("$9\r\nnc:äöü\r\n", chunk);
        Assert.Equal("nc:äöü", unicode.Text);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(3)]
    [InlineData(7)]
    public async Task DoublesBooleansVerbatimAndBigNumbers(int chunk)
    {
        Assert.Equal(3.25, (await ParseAsync(",3.25\r\n", chunk)).Double);
        Assert.Equal(10, (await ParseAsync(",10\r\n", chunk)).Double);
        Assert.Equal(double.PositiveInfinity, (await ParseAsync(",inf\r\n", chunk)).Double);
        Assert.Equal(double.NegativeInfinity, (await ParseAsync(",-inf\r\n", chunk)).Double);
        Assert.True(double.IsNaN((await ParseAsync(",nan\r\n", chunk)).Double));

        Assert.True((await ParseAsync("#t\r\n", chunk)).Boolean);
        Assert.False((await ParseAsync("#f\r\n", chunk)).Boolean);

        var verbatim = await ParseAsync("=15\r\ntxt:Some string\r\n", chunk);
        Assert.Equal(Resp3Kind.Verbatim, verbatim.Kind);
        Assert.Equal("Some string", verbatim.Text);

        var big = await ParseAsync("(3492890328409238509324850943850943825024385\r\n", chunk);
        Assert.Equal(Resp3Kind.BigNumber, big.Kind);
        Assert.Equal("3492890328409238509324850943850943825024385", big.Text);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(3)]
    [InlineData(7)]
    public async Task MapsSetsAndNestedArrays(int chunk)
    {
        // The shape of a HELLO 3 reply, cut down.
        var hello = await ParseAsync("%3\r\n$6\r\nserver\r\n$5\r\nredis\r\n$5\r\nproto\r\n:3\r\n$2\r\nid\r\n:17\r\n", chunk);
        Assert.Equal(Resp3Kind.Map, hello.Kind);
        Assert.Equal(6, hello.Items.Count);
        Assert.Equal(3, hello.MapValue("proto")!.Integer);
        Assert.Equal(17, hello.MapValue("ID")!.Integer); // key match is case-insensitive
        Assert.Equal("redis", hello.MapValue("server")!.Text);
        Assert.Null(hello.MapValue("absent"));

        // The shape of the CLIENT TRACKINGINFO flags: a set of simple strings.
        var info = await ParseAsync("%1\r\n$5\r\nflags\r\n~2\r\n+on\r\n+bcast\r\n", chunk);
        var flags = info.MapValue("flags")!;
        Assert.Equal(Resp3Kind.Set, flags.Kind);
        Assert.Equal(new[] { "on", "bcast" }, flags.Items.Select(f => f.Text));

        var nested = await ParseAsync("*3\r\n*2\r\n:1\r\n:2\r\n$3\r\nfoo\r\n*0\r\n", chunk);
        Assert.Equal(Resp3Kind.Array, nested.Kind);
        Assert.Equal(3, nested.Items.Count);
        Assert.Equal(new long[] { 1, 2 }, nested.Items[0].Items.Select(i => i.Integer));
        Assert.Equal("foo", nested.Items[1].Text);
        Assert.Empty(nested.Items[2].Items);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(3)]
    [InlineData(7)]
    public async Task AttributesAreSkipped(int chunk)
    {
        var value = await ParseAsync("|1\r\n$14\r\nkey-popularity\r\n%1\r\n$1\r\na\r\n,0.19\r\n+OK\r\n", chunk);
        Assert.Equal(Resp3Kind.SimpleString, value.Kind);
        Assert.Equal("OK", value.Text);

        // An attribute can also decorate an element inside an aggregate.
        var array = await ParseAsync("*2\r\n|1\r\n+a\r\n+b\r\n:1\r\n:2\r\n", chunk);
        Assert.Equal(new long[] { 1, 2 }, array.Items.Select(i => i.Integer));
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(3)]
    [InlineData(7)]
    public async Task InvalidationPushFrames(int chunk)
    {
        var push = await ParseAsync(PushOneKey, chunk);
        Assert.Equal(Resp3Kind.Push, push.Kind);
        Assert.Equal(2, push.Items.Count);
        Assert.Equal("invalidate", push.Items[0].Text);
        Assert.Equal(Resp3Kind.Array, push.Items[1].Kind);
        Assert.Equal(new[] { "nc:x" }, push.Items[1].Items.Select(i => i.Text));

        var flush = await ParseAsync(PushNullKeys, chunk);
        Assert.Equal(Resp3Kind.Push, flush.Kind);
        Assert.True(flush.Items[1].IsNull);

        var many = await ParseAsync(">2\r\n$10\r\ninvalidate\r\n*3\r\n$4\r\nnc:a\r\n$4\r\nnc:b\r\n$4\r\nnc:c\r\n", chunk);
        Assert.Equal(new[] { "nc:a", "nc:b", "nc:c" }, many.Items[1].Items.Select(i => i.Text));
    }

    [Fact]
    public async Task BigFrameSurvivesEveryReadBoundary()
    {
        var keys = Enumerable.Range(0, 500).Select(i => $"nc:key:{i:0000}:{new string('p', 40)}").ToArray();
        var frame = new StringBuilder(">2\r\n$10\r\ninvalidate\r\n").Append('*').Append(keys.Length).Append("\r\n");
        foreach (var key in keys) frame.Append('$').Append(key.Length).Append("\r\n").Append(key).Append("\r\n");
        var payload = frame.ToString();

        // One byte at a time: every length prefix, every CRLF and every payload is split.
        var oneByteAtATime = await ParseAsync(payload, 1);
        Assert.Equal(keys, oneByteAtATime.Items[1].Items.Select(i => i.Text).ToArray());

        // And at boundaries that fall wherever a socket would put them.
        for (var seed = 0; seed < 10; seed++)
        {
            var stream = new ChunkedStream(Encoding.UTF8.GetBytes(payload), chunkSize: 0, random: new Random(seed));
            var value = await new Resp3Reader(stream, initialBufferSize: 64).ReadValueAsync(CancellationToken.None);
            Assert.Equal(keys, value.Items[1].Items.Select(i => i.Text).ToArray());
        }
    }

    [Fact]
    public async Task FramesArePipelinedOffOneStream()
    {
        var stream = new ChunkedStream(Encoding.UTF8.GetBytes($"+OK\r\n{PushOneKey}:42\r\n$3\r\nabc\r\n"), chunkSize: 1);
        var reader = new Resp3Reader(stream);

        Assert.Equal("OK", (await reader.ReadValueAsync(CancellationToken.None)).Text);
        Assert.Equal(Resp3Kind.Push, (await reader.ReadValueAsync(CancellationToken.None)).Kind);
        Assert.Equal(42, (await reader.ReadValueAsync(CancellationToken.None)).Integer);
        Assert.Equal("abc", (await reader.ReadValueAsync(CancellationToken.None)).Text);
        await Assert.ThrowsAsync<EndOfStreamException>(async () => await reader.ReadValueAsync(CancellationToken.None));
    }

    [Fact]
    public async Task MalformedFramesThrowProtocolErrors()
    {
        await Assert.ThrowsAsync<Resp3ProtocolException>(async () => await ParseAsync("?2\r\n", 0));
        await Assert.ThrowsAsync<Resp3ProtocolException>(async () => await ParseAsync("$abc\r\n", 0));
        await Assert.ThrowsAsync<Resp3ProtocolException>(async () => await ParseAsync("#x\r\n", 0));
        await Assert.ThrowsAsync<Resp3ProtocolException>(async () => await ParseAsync(",abc\r\n", 0));
        // Truncated mid-frame: the peer went away between the length and the payload.
        await Assert.ThrowsAsync<EndOfStreamException>(async () => await ParseAsync("$10\r\nshort", 0));
        await Assert.ThrowsAsync<EndOfStreamException>(async () => await ParseAsync("*2\r\n:1\r\n", 0));
    }

    [Fact]
    public async Task OversizedAggregateHeadersAreRejectedBeforeAllocating()
    {
        // The element array is allocated before its elements arrive, so a 12-byte header must not be able to ask for
        // hundreds of megabytes; the cap is in elements, separate from the bulk byte cap.
        await Assert.ThrowsAsync<Resp3ProtocolException>(async () => await ParseAsync("*67108864\r\n", 0));
        await Assert.ThrowsAsync<Resp3ProtocolException>(async () => await ParseAsync(">2000000\r\n", 0));
        // A pair count near long.MaxValue used to wrap negative when doubled and slip past the guard.
        await Assert.ThrowsAsync<Resp3ProtocolException>(async () => await ParseAsync("%4611686018427387904\r\n", 0));
        await Assert.ThrowsAsync<Resp3ProtocolException>(async () => await ParseAsync("|9223372036854775807\r\n+OK\r\n", 0));
        // A large but legal aggregate still needs its elements: truncation, not a protocol error.
        await Assert.ThrowsAsync<EndOfStreamException>(async () => await ParseAsync("*1000\r\n", 0));
    }

    [Fact]
    public async Task PushBetweenRepliesDoesNotConsumeAPendingCommand()
    {
        var transport = new DuplexTestStream();
        var pushes = new List<Resp3Value>();
        await using var connection = new Resp3Connection(transport, "test", pushes.Add, NullLogger.Instance);
        connection.Start();

        var ping = connection.ExecuteAsync(CancellationToken.None, "PING");
        await transport.WaitForWritesAsync(1);
        transport.Feed("+PONG\r\n");
        Assert.Equal("PONG", (await ping).Text);

        // A push arriving between the reply to one command and the reply to the next must be routed to the push
        // handler, leaving the FIFO of pending commands alone.
        var id = connection.ExecuteAsync(CancellationToken.None, "CLIENT", "ID");
        await transport.WaitForWritesAsync(2);
        transport.Feed(PushOneKey + PushNullKeys + ":42\r\n");
        Assert.Equal(42, (await id).Integer);

        Assert.Equal(2, pushes.Count);
        Assert.Equal("nc:x", pushes[0].Items[1].Items[0].Text);
        Assert.True(pushes[1].Items[1].IsNull);

        Assert.Equal("*1\r\n$4\r\nPING\r\n*2\r\n$6\r\nCLIENT\r\n$2\r\nID\r\n", transport.Written);
    }

    [Fact]
    public async Task ServerErrorRepliesSurfaceTheServersMessage()
    {
        var transport = new DuplexTestStream();
        await using var connection = new Resp3Connection(transport, "test", static _ => { }, NullLogger.Instance);
        connection.Start();

        var arm = connection.ExecuteAsync(CancellationToken.None, "CLIENT", "TRACKING", "ON", "BCAST", "PREFIX", "a:");
        await transport.WaitForWritesAsync(1);
        transport.Feed("-ERR Prefix 'a:' overlaps with an existing prefix\r\n");

        var error = await Assert.ThrowsAnyAsync<Exception>(async () => await arm);
        Assert.Contains("overlaps with an existing prefix", error.Message);
    }

    [Fact]
    public async Task ClosingTheStreamFaultsPendingCommandsAndCompletesTheConnection()
    {
        var transport = new DuplexTestStream();
        await using var connection = new Resp3Connection(transport, "test", static _ => { }, NullLogger.Instance);
        connection.Start();

        var pending = connection.ExecuteAsync(CancellationToken.None, "PING");
        await transport.WaitForWritesAsync(1);
        transport.FeedEndOfStream();

        await Assert.ThrowsAnyAsync<Exception>(async () => await pending);
        await connection.Completion.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.True(connection.IsClosed);
    }

    [Fact]
    public void CommandsAreEncodedAsArraysOfBulkStrings() =>
        Assert.Equal("*3\r\n$6\r\nCLIENT\r\n$8\r\nTRACKING\r\n$2\r\nON\r\n",
            Encoding.UTF8.GetString(Resp3Connection.Encode("CLIENT", "TRACKING", "ON")));

    [Fact]
    public void OverlappingPrefixesAreDroppedKeepingTheShorter()
    {
        var kept = BroadcastTracker.NormalisePrefixes(new[] { "a:b:", "a:", "nc:", "a:", "nc:x" }, out var dropped);
        Assert.Equal(new[] { "a:", "nc:" }, kept);
        Assert.Equal(new[] { "a:b:", "nc:x" }, dropped);

        Assert.Empty(BroadcastTracker.NormalisePrefixes(Array.Empty<string>(), out var none));
        Assert.Empty(none);

        // Non-overlapping prefixes are all kept, ordered shortest first (then ordinally) for a stable arm command.
        Assert.Equal(new[] { "user:", "cache:" }, BroadcastTracker.NormalisePrefixes(new[] { "cache:", "user:" }, out var noneDropped));
        Assert.Empty(noneDropped);
    }

    private static async Task<Resp3Value> ParseAsync(string payload, int chunk)
    {
        var stream = new ChunkedStream(Encoding.UTF8.GetBytes(payload), chunk);
        return await new Resp3Reader(stream, initialBufferSize: 64).ReadValueAsync(CancellationToken.None);
    }

    /// <summary>A read-only stream that hands the payload out in fixed-size (or random) pieces.</summary>
    private sealed class ChunkedStream(byte[] payload, int chunkSize, Random? random = null) : Stream
    {
        private int _position;

        public override int Read(byte[] buffer, int offset, int count)
        {
            var remaining = payload.Length - _position;
            if (remaining <= 0) return 0;
            var size = chunkSize > 0 ? chunkSize : random?.Next(1, 9) ?? count;
            var take = Math.Min(Math.Min(count, size), remaining);
            Array.Copy(payload, _position, buffer, offset, take);
            _position += take;
            return take;
        }

        public override ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
        {
            var rented = new byte[buffer.Length];
            var read = Read(rented, 0, rented.Length);
            rented.AsMemory(0, read).CopyTo(buffer);
            return ValueTask.FromResult(read);
        }

        public override bool CanRead => true;
        public override bool CanSeek => false;
        public override bool CanWrite => false;
        public override long Length => payload.Length;
        public override long Position { get => _position; set => throw new NotSupportedException(); }
        public override void Flush() { }
        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
        public override void SetLength(long value) => throw new NotSupportedException();
        public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();
    }

    /// <summary>
    /// A stream whose reads block until the test feeds bytes in, and whose writes are captured. Stands in for a
    /// socket so the pending-reply FIFO of <see cref="Resp3Connection"/> can be driven deterministically.
    /// </summary>
    private sealed class DuplexTestStream : Stream
    {
        private readonly Channel<byte[]?> _inbound = Channel.CreateUnbounded<byte[]?>();
        private readonly StringBuilder _written = new();
        private readonly object _sync = new();
        private byte[] _pending = [];
        private int _pendingOffset;
        private int _writes;
        private int _writeTarget;
        private TaskCompletionSource? _writeSignal;
        private bool _ended;

        /// <summary>Everything the connection has written, decoded as UTF-8.</summary>
        public string Written
        {
            get { lock (_sync) { return _written.ToString(); } }
        }

        /// <summary>Makes <paramref name="payload"/> readable by the connection's read loop.</summary>
        public void Feed(string payload) => _inbound.Writer.TryWrite(Encoding.UTF8.GetBytes(payload));

        /// <summary>Closes the read side, as a peer that hung up would.</summary>
        public void FeedEndOfStream() => _inbound.Writer.TryWrite(null);

        /// <summary>Completes once the connection has issued <paramref name="count"/> writes.</summary>
        public Task WaitForWritesAsync(int count)
        {
            lock (_sync)
            {
                if (_writes >= count) return Task.CompletedTask;
                _writeTarget = count;
                _writeSignal = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
                return _writeSignal.Task;
            }
        }

        public override async ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
        {
            while (_pendingOffset >= _pending.Length)
            {
                if (_ended) return 0;
                var next = await _inbound.Reader.ReadAsync(cancellationToken).ConfigureAwait(false);
                if (next is null)
                {
                    _ended = true;
                    return 0;
                }

                _pending = next;
                _pendingOffset = 0;
            }

            var take = Math.Min(buffer.Length, _pending.Length - _pendingOffset);
            _pending.AsMemory(_pendingOffset, take).CopyTo(buffer);
            _pendingOffset += take;
            return take;
        }

        public override ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default)
        {
            lock (_sync)
            {
                _written.Append(Encoding.UTF8.GetString(buffer.Span));
                _writes++;
                if (_writeSignal is not null && _writes >= _writeTarget)
                {
                    _writeSignal.TrySetResult();
                    _writeSignal = null;
                }
            }

            return ValueTask.CompletedTask;
        }

        public override Task FlushAsync(CancellationToken cancellationToken) => Task.CompletedTask;

        protected override void Dispose(bool disposing)
        {
            _inbound.Writer.TryWrite(null);
            base.Dispose(disposing);
        }

        public override bool CanRead => true;
        public override bool CanSeek => false;
        public override bool CanWrite => true;
        public override long Length => throw new NotSupportedException();
        public override long Position { get => throw new NotSupportedException(); set => throw new NotSupportedException(); }
        public override void Flush() { }
        public override int Read(byte[] buffer, int offset, int count) => ReadAsync(buffer.AsMemory(offset, count)).AsTask().GetAwaiter().GetResult();
        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
        public override void SetLength(long value) => throw new NotSupportedException();
        public override void Write(byte[] buffer, int offset, int count) => WriteAsync(buffer.AsMemory(offset, count)).AsTask().GetAwaiter().GetResult();
    }
}
