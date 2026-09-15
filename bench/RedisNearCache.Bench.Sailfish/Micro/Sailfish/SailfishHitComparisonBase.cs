using RedisNearCache.Bench.Contenders;

namespace RedisNearCache.Bench.Micro.Comparisons;

/// <summary>
/// Shared, non-attributed logic for the Sailfish hit-path comparison classes (<c>HitComparisonString</c>,
/// <c>HitComparisonJson</c>). Sailfish discovers <c>[Sailfish*]</c> attributes on the concrete class, so every
/// attributed method lives there as a one-line call into this base -- only the plumbing is shared.
/// <para>
/// Isolation: <see cref="SetupMethod"/> constructs exactly one contender (the one about to be timed) and
/// <see cref="TeardownMethod"/> disposes it before the next method's setup runs, so no other contender's
/// connections are alive while one is being measured.
/// </para>
/// <para>
/// Every attributed wrapper on the concrete classes (<c>[SailfishGlobalSetup]</c>, <c>[SailfishMethodSetup]</c>,
/// <c>[SailfishMethodTeardown]</c>, <c>[SailfishMethod]</c>) must be declared with the <c>async</c>/<c>await</c>
/// keywords, not as an expression-bodied pass-through (<c>Task X() =&gt; Y();</c>): verified directly against
/// Sailfish 4.0.221 that a non-async wrapper returning another method's <c>Task</c> is not reliably awaited before
/// the next lifecycle step runs (state written by an awaited call inside the callee can still be unobserved by the
/// following call), while an <c>async Task X() =&gt; await Y();</c> wrapper is awaited correctly every time.
/// </para>
/// <para>
/// Unlike the BenchmarkDotNet side (<see cref="Micro.HitBenchmarks{T}"/>), the timed <c>[SailfishMethod]</c> itself
/// cannot avoid an async wrapper here -- Sailfish requires one for the same reason the lifecycle hooks do. That
/// wrapper's cost (a state machine plus one <c>Task&lt;object?&gt;</c> allocation) is identical across every
/// contender including the <c>Plain</c> baseline, so it does not bias the Ratio/CI/q-value comparisons between
/// contenders; it is included in every contender's absolute Mean/Median, which Sailfish's own overhead-estimation
/// (see <c>DisableOverheadEstimation</c>) subtracts a per-run baseline call cost from, further reducing it.
/// </para>
/// </summary>
public abstract class SailfishHitComparisonBase
{
    protected abstract DataKind Payload { get; }

    private ContenderSettings _settings = null!;
    private string _key = null!;
    private IContender? _current;

    protected async Task GlobalSetupCore()
    {
        _settings = MicroConfig.BuildSettings("sailfish-hit", TimeSpan.FromSeconds(30));
        _key = $"micro:sailfish:hit:{Payload}";

        await using var seeder = await ContenderRedis.ConnectAsync(_settings, "sailfish-hit-seed", allowAdmin: true);
        var value = Payload == DataKind.String ? ContenderRedis.Encode(Payloads.StringValue) : ContenderRedis.Encode(Payloads.JsonValue);
        await seeder.GetDatabase().StringSetAsync(_key, value);
    }

    protected async Task GlobalTeardownCore()
    {
        await using var admin = await ContenderRedis.ConnectAsync(_settings, "sailfish-hit-cleanup", allowAdmin: true);
        var db = admin.GetDatabase();
        await db.KeyDeleteAsync(_key);
        foreach (var server in admin.GetServers().Where(s => s.IsConnected))
        {
            await foreach (var owned in server.KeysAsync(pattern: ContenderRedis.OwnedKeyPrefix + "*", pageSize: 1000))
            {
                await db.KeyDeleteAsync(owned);
            }
        }
    }

    /// <summary>Connects the one contender under test for the method about to run, reads the key once to populate
    /// its local tier, then asserts a subsequent read completes synchronously when the contender promises it does.</summary>
    protected async Task SetupMethod(ContenderKind kind)
    {
        var contender = ContenderFactory.Create(kind, _settings with { InstanceName = $"sailfish-hit-{kind}" });
        await contender.InitializeAsync();

        if (Payload == DataKind.String) _ = await contender.GetAsync<string>(_key);
        else _ = await contender.GetAsync<SampleRecord>(_key);

        await AssertSynchronousHitAsync(contender, kind);
        _current = contender;
    }

    /// <summary>Mirrors <c>HitBenchmarks.AssertSynchronousHitAsync</c>: a single non-synchronous read right after a
    /// populating miss is not immediately a broken promise (verified directly against FusionCache 2.8.0), so this
    /// retries briefly before failing loudly.</summary>
    private async Task AssertSynchronousHitAsync(IContender contender, ContenderKind kind)
    {
        if (!contender.LocalHitsCompleteSynchronously) return;

        const int maxAttempts = 50;
        for (var attempt = 1; attempt <= maxAttempts; attempt++)
        {
            bool synchronous;
            if (Payload == DataKind.String)
            {
                var vt = contender.GetAsync<string>(_key);
                synchronous = vt.IsCompletedSuccessfully;
                _ = await vt;
            }
            else
            {
                var vt = contender.GetAsync<SampleRecord>(_key);
                synchronous = vt.IsCompletedSuccessfully;
                _ = await vt;
            }

            if (synchronous) return;
            if (attempt < maxAttempts) await Task.Delay(10);
        }

        throw new InvalidOperationException(
            $"{kind}: LocalHitsCompleteSynchronously is true but no read of a primed key completed synchronously in {maxAttempts} attempts");
    }

    protected async Task TeardownMethod()
    {
        if (_current is not null)
        {
            await _current.DisposeAsync();
            _current = null;
        }
    }

    protected async Task<object?> ReadAsync() =>
        Payload == DataKind.String ? await _current!.GetAsync<string>(_key) : await _current!.GetAsync<SampleRecord>(_key);
}
