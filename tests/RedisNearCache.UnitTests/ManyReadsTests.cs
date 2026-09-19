using System.Collections.Concurrent;
using RedisNearCache.Internal;
using Xunit;

namespace RedisNearCache.UnitTests;

/// <summary>
/// <see cref="ManyReads.ReadAsync{TResult}"/> on its own, with a hand-written read delegate instead of a cache:
/// that every read of a window is started before any of them is awaited (the whole point of the fan-out - an
/// implementation that awaited each read in turn would still pass every value-shaped assertion), that no more than
/// <see cref="ManyReads.Window"/> are in flight at once, the argument and cancellation rules, and the failure rule
/// (first failure in key order, rethrown only once every read already started has finished).
/// </summary>
/// <remarks>
/// Every gate here is a <see cref="TaskCompletionSource"/>, never a delay: the tests assert on what has and has not
/// happened at a point the test itself controls, so a slow machine changes nothing. The waits that do have a
/// deadline (<see cref="WaitForAsync"/>) only ever wait for something that must happen, and fail loudly if it does
/// not.
/// </remarks>
public class ManyReadsTests
{
    private static readonly TimeSpan Generous = TimeSpan.FromSeconds(30);

    /// <summary>
    /// Every read must be started before any of them is awaited: all N delegates are entered while none of their
    /// gates has been completed. If the implementation ever awaited read i before starting read i+1, only the first
    /// delegate would be entered and this test would time out waiting for the rest.
    /// </summary>
    [Fact]
    public async Task EveryReadInAWindowStartsBeforeAnyOfThemCompletes()
    {
        const int count = 8;
        var keys = Keys(count);
        var entered = new TaskCompletionSource[count];
        var gates = new TaskCompletionSource<string>[count];
        for (var i = 0; i < count; i++)
        {
            entered[i] = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            gates[i] = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
        }

        var task = ManyReads.ReadAsync<string>(keys, (key, _) =>
        {
            var i = IndexOf(key);
            entered[i].SetResult();
            return new ValueTask<string>(gates[i].Task);
        }, CancellationToken.None).AsTask();

        // Nothing has been released yet, so reaching this line means all eight were started concurrently.
        await Task.WhenAll(entered.Select(e => e.Task)).WaitAsync(Generous);
        Assert.False(task.IsCompleted, "the call completed while every read was still gated.");

        // Completed in reverse order: a result must be filed under its own key, not under the key of whichever
        // read happened to finish first.
        for (var i = count - 1; i >= 0; i--) gates[i].SetResult($"v{i}");

        var result = await task.WaitAsync(Generous);

        Assert.Equal(count, result.Count);
        for (var i = 0; i < count; i++) Assert.Equal($"v{i}", result[keys[i]]);
    }

    /// <summary>
    /// The window bound: with 2*Window+5 keys, exactly Window reads are started up front, the next window starts
    /// only once every read of the first has completed, and the last (short) window holds the remainder.
    /// </summary>
    [Fact]
    public async Task ReadsAreStartedAtMostOneWindowAtATimeAndAWindowWaitsForThePreviousOne()
    {
        const int window = ManyReads.Window;
        var count = (2 * window) + 5;
        var keys = Keys(count);
        var gates = keys.Select(_ => new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously)).ToArray();

        var started = 0;
        var released = 0;
        // For every read, how many gates the test had released when that read was started. A windowed
        // implementation cannot start read number Window until all Window reads before it have completed, i.e.
        // until the test has released them.
        var releasedWhenStarted = new int[count];

        var task = ManyReads.ReadAsync<string>(keys, (key, _) =>
        {
            var i = IndexOf(key);
            releasedWhenStarted[i] = Volatile.Read(ref released);
            Interlocked.Increment(ref started);
            return new ValueTask<string>(gates[i].Task);
        }, CancellationToken.None).AsTask();

        // The start loop runs to completion before the first await, so this is a synchronous fact, not a race:
        // an unwindowed implementation would have started all count reads by now.
        Assert.Equal(window, Volatile.Read(ref started));

        Release(gates, ref released, 0, window);
        Assert.True(await WaitForAsync(() => Volatile.Read(ref started) >= 2 * window),
            $"the second window never started: {Volatile.Read(ref started)} reads started of {count}.");
        Assert.Equal(2 * window, Volatile.Read(ref started));

        Release(gates, ref released, window, window);
        Assert.True(await WaitForAsync(() => Volatile.Read(ref started) >= count),
            $"the third window never started: {Volatile.Read(ref started)} reads started of {count}.");
        Assert.Equal(count, Volatile.Read(ref started));

        Release(gates, ref released, 2 * window, count - (2 * window));
        var result = await task.WaitAsync(Generous);

        for (var i = 0; i < count; i++)
        {
            var expected = i / window * window; // every read of an earlier window must have completed first
            Assert.True(releasedWhenStarted[i] >= expected,
                $"read {i} started with only {releasedWhenStarted[i]} of the previous windows' {expected} reads completed; " +
                $"more than {window} reads were in flight at once.");
        }

        Assert.Equal(count, result.Count);
        for (var i = 0; i < count; i++) Assert.Equal($"v{i}", result[keys[i]]);
    }

    [Fact]
    public async Task DuplicateKeysAreReadOnceAndReturnedOnce()
    {
        var calls = new ConcurrentDictionary<string, int>(StringComparer.Ordinal);

        var result = await ManyReads.ReadAsync<string>(
            ["a", "b", "a", "a"],
            (key, _) =>
            {
                calls.AddOrUpdate(key, 1, static (_, n) => n + 1);
                return new ValueTask<string>("v:" + key);
            },
            CancellationToken.None);

        Assert.Equal(2, result.Count);
        Assert.Equal("v:a", result["a"]);
        Assert.Equal("v:b", result["b"]);
        Assert.Equal(1, calls["a"]);
        Assert.Equal(1, calls["b"]);
    }

    [Fact]
    public async Task KeysAreComparedOrdinallySoCaseMakesTwoEntries()
    {
        var calls = new List<string>();

        var result = await ManyReads.ReadAsync<string>(
            ["Key", "key"],
            (key, _) =>
            {
                lock (calls) calls.Add(key);
                return new ValueTask<string>("v:" + key);
            },
            CancellationToken.None);

        Assert.Equal(2, result.Count);
        Assert.Equal("v:Key", result["Key"]);
        Assert.Equal("v:key", result["key"]);
        Assert.Equal(2, calls.Count);
    }

    [Fact]
    public async Task NullKeysThrowsArgumentNullException()
    {
        var called = false;

        var ex = await Assert.ThrowsAsync<ArgumentNullException>(
            () => ManyReads.ReadAsync<string>(null!, (_, _) => { called = true; return new ValueTask<string>("v"); }, CancellationToken.None).AsTask());

        Assert.Equal("keys", ex.ParamName);
        Assert.False(called, "no read may be started for a null sequence.");
    }

    /// <summary>
    /// The null element is deliberately the LAST one: the sequence is validated in full before the first read, so
    /// a bad key is never discovered with earlier reads already on the wire.
    /// </summary>
    [Fact]
    public async Task ANullKeyAtTheEndThrowsBeforeAnyReadIsStarted()
    {
        var called = false;

        var ex = await Assert.ThrowsAsync<ArgumentException>(
            () => ManyReads.ReadAsync<string>(
                ["a", "b", null!],
                (_, _) => { called = true; return new ValueTask<string>("v"); },
                CancellationToken.None).AsTask());

        Assert.Equal("keys", ex.ParamName);
        Assert.False(called, "the reads for the keys before the null one must not have been started.");
    }

    [Fact]
    public async Task TheCallersSequenceIsEnumeratedExactlyOnce()
    {
        var source = new CountingEnumerable(["a", "b", "c"]);

        var result = await ManyReads.ReadAsync<string>(source, (key, _) => new ValueTask<string>("v:" + key), CancellationToken.None);

        Assert.Equal(3, result.Count);
        Assert.Equal(1, source.Enumerations);
    }

    [Fact]
    public async Task EmptyKeysReturnsAnEmptyResultAndReadsNothing()
    {
        var called = false;

        var result = await ManyReads.ReadAsync<string>(
            Array.Empty<string>(),
            (_, _) => { called = true; return new ValueTask<string>("v"); },
            CancellationToken.None);

        Assert.Empty(result);
        Assert.False(called, "an empty key list must not produce a read.");
    }

    [Fact]
    public async Task APreCancelledTokenThrowsBeforeAnyReadIsStarted()
    {
        var called = false;

        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => ManyReads.ReadAsync<string>(
                ["a", "b"],
                (_, _) => { called = true; return new ValueTask<string>("v"); },
                new CancellationToken(true)).AsTask());

        Assert.False(called, "a pre-cancelled call must not start a read.");
    }

    [Fact]
    public async Task ATokenCancelledBetweenWindowsStopsTheNextWindow()
    {
        const int window = ManyReads.Window;
        var count = window + 5;
        var keys = Keys(count);
        var gates = keys.Select(_ => new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously)).ToArray();
        var started = 0;
        var released = 0;
        using var cts = new CancellationTokenSource();

        var task = ManyReads.ReadAsync<string>(keys, (key, _) =>
        {
            Interlocked.Increment(ref started);
            return new ValueTask<string>(gates[IndexOf(key)].Task);
        }, cts.Token).AsTask();

        Assert.Equal(window, Volatile.Read(ref started));

        await cts.CancelAsync();
        Release(gates, ref released, 0, window);

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => task.WaitAsync(Generous));
        Assert.Equal(window, Volatile.Read(ref started));
    }

    /// <summary>
    /// Two reads fail with different exception types while the other eight are still gated. The call must not
    /// complete until every read it started has finished (no task left unobserved, no in-flight token left held),
    /// and must then rethrow the FIRST failure in key order - the original instance, not an
    /// <see cref="AggregateException"/> wrapping both.
    /// </summary>
    [Fact]
    public async Task TheFirstFailureInKeyOrderIsRethrownOnlyAfterEveryStartedReadHasFinished()
    {
        var keys = Keys(10);
        var gates = new Dictionary<string, TaskCompletionSource<string>>(StringComparer.Ordinal);
        var k3 = new InvalidOperationException("k3 failed");
        var k7 = new FormatException("k7 failed");

        var task = ManyReads.ReadAsync<string>(keys, (key, _) =>
        {
            var i = IndexOf(key);
            if (i == 3) return ValueTask.FromException<string>(k3);
            if (i == 7) return ValueTask.FromException<string>(k7);
            var gate = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
            lock (gates) gates[key] = gate;
            return new ValueTask<string>(gate.Task);
        }, CancellationToken.None).AsTask();

        Assert.Equal(8, gates.Count);
        var gated = gates.OrderBy(g => IndexOf(g.Key)).ToArray();
        for (var i = 0; i < gated.Length - 1; i++)
        {
            Assert.False(task.IsCompleted,
                $"the call reported failure while {gated.Length - i} of the reads it started were still running.");
            gated[i].Value.SetResult("v");
        }

        Assert.False(task.IsCompleted, "the call reported failure while the last of the reads it started was still running.");
        gated[^1].Value.SetResult("v");

        var thrown = await Assert.ThrowsAsync<InvalidOperationException>(() => task.WaitAsync(Generous));

        Assert.Same(k3, thrown);
    }

    [Fact]
    public async Task ADelegateThatThrowsSynchronouslyIsHeldLikeAFaultedTask()
    {
        var keys = Keys(4);
        var gates = new Dictionary<string, TaskCompletionSource<string>>(StringComparer.Ordinal);
        var boom = new InvalidOperationException("k1 threw synchronously");

        var task = ManyReads.ReadAsync<string>(keys, (key, _) =>
        {
            if (IndexOf(key) == 1) throw boom;
            var gate = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
            lock (gates) gates[key] = gate;
            return new ValueTask<string>(gate.Task);
        }, CancellationToken.None).AsTask();

        // The synchronous throw did not abandon the reads around it, and did not surface before they finished.
        Assert.Equal(3, gates.Count);
        Assert.False(task.IsCompleted, "a synchronous throw must not end the call while the other reads are still running.");
        foreach (var gate in gates.Values) gate.SetResult("v");

        var thrown = await Assert.ThrowsAsync<InvalidOperationException>(() => task.WaitAsync(Generous));
        Assert.Same(boom, thrown);
    }

    [Fact]
    public async Task AFailureInTheFirstWindowStopsTheSecondOne()
    {
        const int window = ManyReads.Window;
        var count = window + 5;
        var keys = Keys(count);
        var started = 0;
        var boom = new InvalidOperationException("k0 failed");

        var task = ManyReads.ReadAsync<string>(keys, (key, _) =>
        {
            Interlocked.Increment(ref started);
            return IndexOf(key) == 0 ? ValueTask.FromException<string>(boom) : new ValueTask<string>("v");
        }, CancellationToken.None).AsTask();

        var thrown = await Assert.ThrowsAsync<InvalidOperationException>(() => task.WaitAsync(Generous));

        Assert.Same(boom, thrown);
        Assert.Equal(window, Volatile.Read(ref started));
    }

    [Fact]
    public async Task AReadThatReturnsNullPutsTheKeyInTheResultWithANullValue()
    {
        var result = await ManyReads.ReadAsync<string?>(
            ["present", "missing"],
            (key, _) => new ValueTask<string?>(key == "missing" ? null : "v"),
            CancellationToken.None);

        Assert.Equal(2, result.Count);
        Assert.True(result.ContainsKey("missing"), "a key whose read returned null must still be present in the result.");
        Assert.Null(result["missing"]);
        Assert.Equal("v", result["present"]);
    }

    [Fact]
    public async Task AReadThatReturnsTheDefaultOfAValueTypePutsTheKeyInTheResult()
    {
        var result = await ManyReads.ReadAsync<int?>(
            ["zero", "missing"],
            (key, _) => new ValueTask<int?>(key == "missing" ? null : 0),
            CancellationToken.None);

        Assert.Equal(2, result.Count);
        Assert.Null(result["missing"]);
        Assert.Equal(0, result["zero"]);
    }

    /// <summary>
    /// The documented trap: for a non-nullable value type a missing key reads as the type's default, exactly as
    /// GetAsync does, which is why the docs point at the nullable form.
    /// </summary>
    [Fact]
    public async Task ForANonNullableValueTypeAMissingKeyIsIndistinguishableFromAStoredDefault()
    {
        IReadOnlyDictionary<string, int> result = await ManyReads.ReadAsync<int>(
            ["zero", "missing"],
            (_, _) => new ValueTask<int>(0),
            CancellationToken.None);

        Assert.Equal(2, result.Count);
        Assert.Equal(0, result["zero"]);
        Assert.Equal(0, result["missing"]);
    }

    [Fact]
    public async Task TheTokenGivenToTheCallIsTheOneEveryReadReceives()
    {
        using var cts = new CancellationTokenSource();
        var seen = new List<CancellationToken>();

        var result = await ManyReads.ReadAsync<string>(["a", "b"], (_, token) =>
        {
            lock (seen) seen.Add(token);
            return new ValueTask<string>("v");
        }, cts.Token);

        Assert.Equal(2, result.Count);
        Assert.Equal(2, seen.Count);
        Assert.All(seen, token => Assert.Equal(cts.Token, token));
    }

    private static string[] Keys(int count) => Enumerable.Range(0, count).Select(i => $"k{i}").ToArray();

    /// <summary>The index encoded in a key built by <see cref="Keys"/>.</summary>
    private static int IndexOf(string key) => int.Parse(key.AsSpan(1));

    private static void Release(TaskCompletionSource<string>[] gates, ref int released, int start, int count)
    {
        for (var i = start; i < start + count; i++)
        {
            // Counted BEFORE the gate opens, so a read started by the resuming continuation can never see a
            // release count that is lower than the number of reads actually finished.
            Interlocked.Increment(ref released);
            gates[i].SetResult($"v{i}");
        }
    }

    private static async Task<bool> WaitForAsync(Func<bool> condition)
    {
        var deadline = DateTime.UtcNow + Generous;
        while (!condition())
        {
            if (DateTime.UtcNow >= deadline) return condition();
            await Task.Delay(5);
        }

        return true;
    }

    /// <summary>An <see cref="IEnumerable{T}"/> that counts how many times it has been enumerated.</summary>
    private sealed class CountingEnumerable(IReadOnlyList<string> items) : IEnumerable<string>
    {
        private int _enumerations;

        public int Enumerations => Volatile.Read(ref _enumerations);

        public IEnumerator<string> GetEnumerator()
        {
            Interlocked.Increment(ref _enumerations);
            return items.GetEnumerator();
        }

        System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator() => GetEnumerator();
    }
}
