using System.Runtime.ExceptionServices;

namespace RedisNearCache.Internal;

/// <summary>
/// The multi-key read behind <see cref="IRedisNearCache.GetManyAsync{T}"/> and
/// <see cref="IRedisNearCache.GetManyBytesAsync"/>: the single-key read, started for every distinct key before any
/// of them is awaited.
/// </summary>
/// <remarks>
/// Deliberately not an <c>MGET</c>. A single-key read writes its commands to the multiplexer before its first
/// await, so starting N of them back to back pipelines them: one round trip per node, as an <c>MGET</c> would be.
/// What this keeps, by going through the one read path there is, is everything that path guarantees per key: the
/// in-flight token that discards a reply overtaken by an invalidation, the TTL cap and its fallbacks, the untracked
/// read for a key outside <c>KeyPrefixes</c>, pass-through while tracking is lost, and slot routing on a cluster
/// (where an <c>MGET</c> across slots is refused). An <c>MGET</c> would need all of that a second time.
/// </remarks>
internal static class ManyReads
{
    /// <summary>
    /// How many single-key reads are in flight at once. Each is up to two commands (<c>GET</c> and <c>PTTL</c>), and
    /// StackExchange.Redis times a command from the moment it is queued: without a bound, a call for tens of
    /// thousands of keys would queue them all at once and time out its own tail.
    /// </summary>
    internal const int Window = 256;

    /// <summary>
    /// Reads every distinct key in <paramref name="keys"/> through <paramref name="read"/>, <see cref="Window"/> at a
    /// time, and returns one entry per distinct key. If any read fails, the first failure (in key order) is
    /// rethrown - but only once every read already started has finished, so none is left unobserved or still
    /// holding its in-flight token - and no further window is started.
    /// </summary>
    public static async ValueTask<IReadOnlyDictionary<string, TResult>> ReadAsync<TResult>(
        IEnumerable<string> keys,
        Func<string, CancellationToken, ValueTask<TResult>> read,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(keys);

        // Snapshot and validate before the first read: a bad element must not be discovered with half the reads
        // already on the wire, and the caller's sequence is enumerated exactly once.
        var distinct = new List<string>();
        var seen = new HashSet<string>(StringComparer.Ordinal);
        foreach (var key in keys)
        {
            if (key is null) throw new ArgumentException("The keys must not contain null.", nameof(keys));
            if (seen.Add(key)) distinct.Add(key);
        }

        cancellationToken.ThrowIfCancellationRequested();

        // Ordinal, as Redis compares keys: byte for byte.
        var results = new Dictionary<string, TResult>(distinct.Count, StringComparer.Ordinal);
        if (distinct.Count == 0) return results;

        var pending = new ValueTask<TResult>[Math.Min(Window, distinct.Count)];
        for (var start = 0; start < distinct.Count; start += Window)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var count = Math.Min(Window, distinct.Count - start);

            // Start them all before awaiting any: that is what puts them on the wire together.
            for (var i = 0; i < count; i++)
            {
                try
                {
                    pending[i] = read(distinct[start + i], cancellationToken);
                }
                catch (Exception ex)
                {
                    // An implementation that throws instead of returning a faulted task. Held like any other
                    // failure, so the reads already started are still awaited below.
                    pending[i] = ValueTask.FromException<TResult>(ex);
                }
            }

            ExceptionDispatchInfo? failure = null;
            for (var i = 0; i < count; i++)
            {
                try
                {
                    results[distinct[start + i]] = await pending[i].ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    failure ??= ExceptionDispatchInfo.Capture(ex);
                }

                pending[i] = default;
            }

            failure?.Throw();
        }

        return results;
    }
}
