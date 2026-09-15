using System.Globalization;
using System.Text;

namespace RedisNearCache.Tracking.Broadcast;

/// <summary>A RESP3 frame that could not be decoded: a bad type byte, a bad length, or a truncated stream.</summary>
internal sealed class Resp3ProtocolException : Exception
{
    /// <summary>Creates the exception with a message.</summary>
    public Resp3ProtocolException(string message) : base(message) { }

    /// <summary>Creates the exception with a message and the error that caused it.</summary>
    public Resp3ProtocolException(string message, Exception? innerException) : base(message, innerException) { }
}

/// <summary>
/// Decodes RESP3 frames from a <see cref="Stream"/>, one at a time, with its own re-used buffer.
/// </summary>
/// <remarks>
/// Frames may be split across any number of reads: the reader only ever asks the stream for more bytes when the
/// bytes it already holds are not enough, so a frame delivered one byte at a time decodes exactly like one that
/// arrives whole. Attribute frames (<c>|</c>) are decoded and discarded, so the caller never sees them; every
/// other RESP3 type is returned as a <see cref="Resp3Value"/>. Not thread-safe: the owner reads on one loop.
/// </remarks>
internal sealed class Resp3Reader
{
    /// <summary>Guards against a hostile or corrupt length prefix; real replies are orders of magnitude smaller.</summary>
    private const int MaxBulkLength = 64 * 1024 * 1024;

    /// <summary>Guards against unbounded recursion on a corrupt stream of nested aggregates.</summary>
    private const int MaxDepth = 64;

    /// <summary>
    /// Largest aggregate accepted, in elements: the array is allocated before its elements are read, so this must
    /// be far smaller than the byte cap. A multi-key invalidation is the largest aggregate this socket ever sees.
    /// </summary>
    private const int MaxAggregateLength = 1 << 20;

    private readonly Stream _stream;
    private byte[] _buffer;
    private int _start;
    private int _end;

    /// <summary>Creates a reader over <paramref name="stream"/>.</summary>
    /// <param name="stream">The stream to decode from; the reader never closes it.</param>
    /// <param name="initialBufferSize">Initial size of the internal buffer. It grows on demand.</param>
    public Resp3Reader(Stream stream, int initialBufferSize = 4096)
    {
        _stream = stream;
        _buffer = new byte[Math.Max(64, initialBufferSize)];
    }

    /// <summary>
    /// Decodes the next frame. Throws <see cref="EndOfStreamException"/> when the peer closed the connection
    /// between frames, and <see cref="Resp3ProtocolException"/> when the bytes are not valid RESP3.
    /// </summary>
    public async ValueTask<Resp3Value> ReadValueAsync(CancellationToken cancellationToken) =>
        await ReadValueAsync(0, cancellationToken).ConfigureAwait(false);

    private async ValueTask<Resp3Value> ReadValueAsync(int depth, CancellationToken cancellationToken)
    {
        if (depth > MaxDepth) throw new Resp3ProtocolException($"RESP3 frame nested deeper than {MaxDepth} levels.");

        while (true)
        {
            var type = await ReadTypeByteAsync(cancellationToken).ConfigureAwait(false);
            switch (type)
            {
                case (byte)'+':
                    return Resp3Value.String(Resp3Kind.SimpleString, await ReadLineAsync(cancellationToken).ConfigureAwait(false));
                case (byte)'-':
                    return Resp3Value.String(Resp3Kind.Error, await ReadLineAsync(cancellationToken).ConfigureAwait(false));
                case (byte)':':
                    return Resp3Value.FromInteger(await ReadLongAsync(cancellationToken).ConfigureAwait(false));
                case (byte)'(':
                    return Resp3Value.String(Resp3Kind.BigNumber, await ReadLineAsync(cancellationToken).ConfigureAwait(false));
                case (byte)'_':
                    await ReadLineAsync(cancellationToken).ConfigureAwait(false);
                    return Resp3Value.Null;
                case (byte)'#':
                    {
                        var line = await ReadLineAsync(cancellationToken).ConfigureAwait(false);
                        return line switch
                        {
                            "t" => Resp3Value.FromBoolean(true),
                            "f" => Resp3Value.FromBoolean(false),
                            _ => throw new Resp3ProtocolException($"RESP3 boolean was '{line}', expected 't' or 'f'."),
                        };
                    }
                case (byte)',':
                    {
                        var line = await ReadLineAsync(cancellationToken).ConfigureAwait(false);
                        return Resp3Value.FromDouble(ParseDouble(line));
                    }
                case (byte)'$':
                case (byte)'=':
                case (byte)'!':
                    {
                        var length = await ReadLongAsync(cancellationToken).ConfigureAwait(false);
                        if (length < 0) return Resp3Value.Null; // RESP2 null bulk string
                        if (length > MaxBulkLength) throw new Resp3ProtocolException($"RESP3 bulk length {length} exceeds the {MaxBulkLength} byte limit.");
                        var text = await ReadBulkAsync((int)length, cancellationToken).ConfigureAwait(false);
                        return type switch
                        {
                            (byte)'$' => Resp3Value.String(Resp3Kind.BulkString, text),
                            (byte)'!' => Resp3Value.String(Resp3Kind.Error, text),
                            _ => Resp3Value.String(Resp3Kind.Verbatim, StripVerbatimFormat(text)),
                        };
                    }
                case (byte)'*':
                case (byte)'~':
                case (byte)'>':
                    {
                        var count = await ReadLongAsync(cancellationToken).ConfigureAwait(false);
                        if (count < 0) return Resp3Value.Null; // RESP2 null array
                        var items = await ReadItemsAsync(count, depth, cancellationToken).ConfigureAwait(false);
                        var kind = type switch
                        {
                            (byte)'*' => Resp3Kind.Array,
                            (byte)'~' => Resp3Kind.Set,
                            _ => Resp3Kind.Push,
                        };
                        return Resp3Value.Aggregate(kind, items);
                    }
                case (byte)'%':
                    {
                        var pairs = await ReadLongAsync(cancellationToken).ConfigureAwait(false);
                        if (pairs < 0) return Resp3Value.Null;
                        if (pairs > MaxAggregateLength / 2) throw new Resp3ProtocolException($"RESP3 map of {pairs} pairs exceeds the {MaxAggregateLength} element limit.");
                        var items = await ReadItemsAsync(pairs * 2, depth, cancellationToken).ConfigureAwait(false);
                        return Resp3Value.Aggregate(Resp3Kind.Map, items);
                    }
                case (byte)'|':
                    {
                        // An attribute decorates the frame that follows it. Nothing we send asks for attributes,
                        // so decode it to keep the stream in sync and then read the real frame.
                        var pairs = await ReadLongAsync(cancellationToken).ConfigureAwait(false);
                        if (pairs > MaxAggregateLength / 2) throw new Resp3ProtocolException($"RESP3 attribute of {pairs} pairs exceeds the {MaxAggregateLength} element limit.");
                        if (pairs > 0) await ReadItemsAsync(pairs * 2, depth, cancellationToken).ConfigureAwait(false);
                        continue;
                    }
                default:
                    throw new Resp3ProtocolException($"Unknown RESP3 type byte 0x{type:x2} ('{(char)type}').");
            }
        }
    }

    private async ValueTask<Resp3Value[]> ReadItemsAsync(long count, int depth, CancellationToken cancellationToken)
    {
        if (count > MaxAggregateLength) throw new Resp3ProtocolException($"RESP3 aggregate of {count} elements exceeds the {MaxAggregateLength} element limit.");
        var items = count == 0 ? Array.Empty<Resp3Value>() : new Resp3Value[count];
        for (var i = 0; i < count; i++)
        {
            items[i] = await ReadValueAsync(depth + 1, cancellationToken).ConfigureAwait(false);
        }

        return items;
    }

    /// <summary>A verbatim string carries a three-character format and a colon before its content; callers want the content.</summary>
    private static string StripVerbatimFormat(string text) =>
        text.Length >= 4 && text[3] == ':' ? text[4..] : text;

    private static double ParseDouble(string line) => line switch
    {
        "inf" or "+inf" => double.PositiveInfinity,
        "-inf" => double.NegativeInfinity,
        "nan" or "-nan" => double.NaN,
        _ => double.TryParse(line, NumberStyles.Float, CultureInfo.InvariantCulture, out var value)
            ? value
            : throw new Resp3ProtocolException($"RESP3 double was '{line}'."),
    };

    private async ValueTask<byte> ReadTypeByteAsync(CancellationToken cancellationToken)
    {
        while (_end == _start) await FillAsync(cancellationToken).ConfigureAwait(false);
        return _buffer[_start++];
    }

    private async ValueTask<long> ReadLongAsync(CancellationToken cancellationToken)
    {
        var line = await ReadLineAsync(cancellationToken).ConfigureAwait(false);
        return long.TryParse(line, NumberStyles.Integer, CultureInfo.InvariantCulture, out var value)
            ? value
            : throw new Resp3ProtocolException($"RESP3 length or integer was '{line}'.");
    }

    /// <summary>Reads <paramref name="length"/> bytes of payload plus the trailing CRLF.</summary>
    private async ValueTask<string> ReadBulkAsync(int length, CancellationToken cancellationToken)
    {
        await EnsureAsync(length + 2, cancellationToken).ConfigureAwait(false);
        var text = Encoding.UTF8.GetString(_buffer, _start, length);
        _start += length;
        if (_buffer[_start] != (byte)'\r' || _buffer[_start + 1] != (byte)'\n')
            throw new Resp3ProtocolException("RESP3 bulk string was not followed by CRLF.");
        _start += 2;
        return text;
    }

    /// <summary>Reads up to the next LF and returns everything before it, without the CR.</summary>
    private async ValueTask<string> ReadLineAsync(CancellationToken cancellationToken)
    {
        var searched = 0;
        while (true)
        {
            var offset = IndexOfNewline(_start + searched);
            if (offset >= 0)
            {
                var index = _start + searched + offset;
                var length = index - _start;
                if (length > 0 && _buffer[index - 1] == (byte)'\r') length--;
                var text = Encoding.UTF8.GetString(_buffer, _start, length);
                _start = index + 1;
                return text;
            }

            searched = _end - _start;
            await FillAsync(cancellationToken).ConfigureAwait(false);
        }
    }

    /// <summary>Index of the next LF at or after <paramref name="from"/>, relative to it, or -1.</summary>
    private int IndexOfNewline(int from) => _buffer.AsSpan(from, _end - from).IndexOf((byte)'\n');

    /// <summary>Makes sure at least <paramref name="count"/> bytes are buffered, reading more as needed.</summary>
    private async ValueTask EnsureAsync(int count, CancellationToken cancellationToken)
    {
        while (_end - _start < count)
        {
            Grow(count);
            await FillAsync(cancellationToken).ConfigureAwait(false);
        }
    }

    /// <summary>Compacts, and if that is not enough grows, the buffer so it can hold <paramref name="count"/> more bytes.</summary>
    private void Grow(int count)
    {
        if (_buffer.Length >= count && _buffer.Length - _start >= count) return;
        Compact();
        if (_buffer.Length >= count) return;

        var size = _buffer.Length;
        while (size < count) size *= 2;
        var grown = new byte[size];
        Buffer.BlockCopy(_buffer, _start, grown, 0, _end - _start);
        _end -= _start;
        _start = 0;
        _buffer = grown;
    }

    private void Compact()
    {
        if (_start == 0) return;
        if (_end > _start) Buffer.BlockCopy(_buffer, _start, _buffer, 0, _end - _start);
        _end -= _start;
        _start = 0;
    }

    /// <summary>Reads at least one more byte from the stream into the buffer.</summary>
    private async ValueTask FillAsync(CancellationToken cancellationToken)
    {
        if (_start == _end)
        {
            _start = 0;
            _end = 0;
        }
        else if (_end == _buffer.Length)
        {
            Grow(_buffer.Length + 1);
        }

        var read = await _stream.ReadAsync(_buffer.AsMemory(_end, _buffer.Length - _end), cancellationToken).ConfigureAwait(false);
        if (read <= 0) throw new EndOfStreamException("The RESP3 connection was closed by the peer.");
        _end += read;
    }
}
