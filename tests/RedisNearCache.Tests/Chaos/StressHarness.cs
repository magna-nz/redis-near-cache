using System.Collections.Concurrent;
using System.Globalization;

namespace RedisNearCache.Tests.Chaos;

internal sealed class StressOutcome
{
    public long Reads;
    public long Writes;

    public ConcurrentBag<Exception> ReaderFailures { get; } = [];

    /// <summary>
    /// The last value written to each key, recorded only after the write completed. The harness uses a single
    /// writer task, so "last" is unambiguous: there is a total order over the writes.
    /// </summary>
    public Dictionary<string, string> FinalValues { get; } = [];

    public ConcurrentBag<string> Violations { get; } = [];
}

/// <summary>
/// The read side shared by the stress tests: N reader tasks hammering <see cref="IRedisNearCache.GetAsync{T}"/>
/// over a pool of keys while one writer task rewrites random keys with monotonically increasing values for a
/// fixed window. The writer is what varies between tests (a foreign client, the cache's own
/// <see cref="IRedisNearCache.SetAsync{T}(string, T, TimeSpan?, CancellationToken)"/>), so it is passed in.
/// </summary>
internal static class StressHarness
{
    public static async Task<StressOutcome> RunAsync(
        IRedisNearCache cache,
        IReadOnlyList<string> keys,
        int readerCount,
        TimeSpan duration,
        Func<string, string, Task> writeAsync,
        Action<StressOutcome, string, string?>? inspectRead = null)
    {
        var outcome = new StressOutcome();
        using var stop = new CancellationTokenSource();

        var readers = new Task[readerCount];
        for (var r = 0; r < readerCount; r++)
        {
            var seed = r;
            readers[r] = Task.Run(async () =>
            {
                var rng = new Random(seed * 7919 + 13);
                while (!stop.IsCancellationRequested)
                {
                    var key = keys[rng.Next(keys.Count)];
                    try
                    {
                        var value = await cache.GetAsync<string>(key);
                        Interlocked.Increment(ref outcome.Reads);
                        inspectRead?.Invoke(outcome, key, value);
                    }
                    catch (OperationCanceledException) when (stop.IsCancellationRequested)
                    {
                        // shutting the harness down
                    }
                    catch (Exception ex)
                    {
                        outcome.ReaderFailures.Add(ex);
                    }

                    // A hit is served synchronously, so without an explicit yield 32 of these loops would
                    // monopolise the thread pool and starve the invalidation callbacks we are measuring.
                    await Task.Yield();
                }
            });
        }

        var writer = Task.Run(async () =>
        {
            var rng = new Random(4242);
            long counter = 0;
            var deadline = DateTime.UtcNow + duration;
            while (DateTime.UtcNow < deadline)
            {
                var key = keys[rng.Next(keys.Count)];
                var value = (++counter).ToString(CultureInfo.InvariantCulture);
                await writeAsync(key, value);
                outcome.FinalValues[key] = value;
                outcome.Writes++;
            }
        });

        await writer;
        await stop.CancelAsync();
        await Task.WhenAll(readers);
        return outcome;
    }

    public static IReadOnlyList<string> KeyPool(string label, int count)
    {
        var prefix = TestHelpers.Key(label);
        return Enumerable.Range(0, count).Select(i => $"{prefix}:{i}").ToArray();
    }
}
