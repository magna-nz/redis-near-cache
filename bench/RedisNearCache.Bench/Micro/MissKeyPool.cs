using RedisNearCache.Bench.Contenders;
using StackExchange.Redis;

namespace RedisNearCache.Bench.Micro;

/// <summary>
/// A pool of distinct source-of-truth keys (<c>micro:miss:{payload}:{i}</c>) for the miss-path benchmarks: each has no
/// L2 entry and has never been read by the contender instance under test, so every draw is a genuine local miss, L2
/// miss (where applicable) and source load. <see cref="Next"/> hands keys out sequentially and throws instead of
/// wrapping once the pool this run pre-sized is exhausted.
/// </summary>
public sealed class MissKeyPool : IAsyncDisposable
{
    private readonly IConnectionMultiplexer _admin;
    private readonly string[] _keys;
    private int _next = -1;

    private MissKeyPool(IConnectionMultiplexer admin, string[] keys)
    {
        _admin = admin;
        _keys = keys;
    }

    /// <summary>Deletes leftover <c>micro:miss:*</c> and <c>bench:l2:*</c> keys from an earlier run, then seeds
    /// <paramref name="count"/> fresh distinct keys of the given payload shape.</summary>
    public static async Task<MissKeyPool> CreateAsync(ContenderSettings settings, DataKind payload, int count, CancellationToken ct = default)
    {
        var admin = await ContenderRedis.ConnectAsync(settings, "bench-micro-miss-admin", allowAdmin: true);
        await DeleteByPatternAsync(admin, "micro:miss:*");
        await DeleteByPatternAsync(admin, ContenderRedis.OwnedKeyPrefix + "*");

        var db = admin.GetDatabase();
        var keys = new string[count];
        var value = payload == DataKind.String ? ContenderRedis.Encode(Payloads.StringValue) : ContenderRedis.Encode(Payloads.JsonValue);
        var tasks = new Task[count];
        for (var i = 0; i < count; i++)
        {
            keys[i] = $"micro:miss:{payload}:{i}";
            tasks[i] = db.StringSetAsync(keys[i], value);
        }
        await Task.WhenAll(tasks);
        return new MissKeyPool(admin, keys);
    }

    /// <summary>The next never-yet-drawn key. Throws when the pool sized for this job is exhausted, rather than
    /// silently reusing a key (which would turn a miss into a hit on a second draw).</summary>
    public string Next()
    {
        var i = Interlocked.Increment(ref _next);
        if (i >= _keys.Length)
        {
            throw new InvalidOperationException(
                $"miss key pool exhausted ({_keys.Length} keys pre-seeded); size the pool from the job's max invocations.");
        }
        return _keys[i];
    }

    public async ValueTask DisposeAsync()
    {
        var db = _admin.GetDatabase();
        await Task.WhenAll(_keys.Select(k => db.KeyDeleteAsync(k)));
        await DeleteByPatternAsync(_admin, ContenderRedis.OwnedKeyPrefix + "*");
        await _admin.DisposeAsync();
    }

    private static async Task DeleteByPatternAsync(IConnectionMultiplexer mux, string pattern)
    {
        var db = mux.GetDatabase();
        foreach (var server in mux.GetServers().Where(s => s.IsConnected))
        {
            var batch = new List<Task<bool>>();
            await foreach (var key in server.KeysAsync(pattern: pattern, pageSize: 1000))
            {
                batch.Add(db.KeyDeleteAsync(key));
                if (batch.Count >= 1000) { await Task.WhenAll(batch); batch.Clear(); }
            }
            if (batch.Count > 0) await Task.WhenAll(batch);
        }
    }
}
