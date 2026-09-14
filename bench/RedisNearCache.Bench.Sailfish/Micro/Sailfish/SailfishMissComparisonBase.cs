using RedisNearCache.Bench.Contenders;

namespace RedisNearCache.Bench.Micro.Comparisons;

/// <summary>
/// Shared, non-attributed logic for the Sailfish miss-path comparison classes (<c>MissComparisonString</c>,
/// <c>MissComparisonJson</c>). See <see cref="SailfishHitComparisonBase"/> for why attributed members live on the
/// concrete classes and only call into shared base logic.
/// <para>
/// Isolation: <see cref="SetupMethod"/> constructs exactly one fresh contender per method; each sample reads a
/// distinct never-before-read key drawn from <see cref="MissKeyPool"/>, so every read is a genuine local miss / L2
/// miss / source load regardless of how many samples Sailfish takes.
/// </para>
/// </summary>
public abstract class SailfishMissComparisonBase
{
    protected abstract DataKind Payload { get; }

    private ContenderSettings _settings = null!;
    private MissKeyPool _pool = null!;
    private IContender? _current;

    protected async Task GlobalSetupCore()
    {
        _settings = MicroConfig.BuildSettings("sailfish-miss", TimeSpan.FromSeconds(30));
        var poolSize = MicroJobSettings.SailfishMissPoolSize(MicroConfig.Quick);
        _pool = await MissKeyPool.CreateAsync(_settings, Payload, poolSize);
    }

    protected async Task GlobalTeardownCore() => await _pool.DisposeAsync();

    protected async Task SetupMethod(ContenderKind kind)
    {
        var contender = ContenderFactory.Create(kind, _settings with { InstanceName = $"sailfish-miss-{kind}" });
        await contender.InitializeAsync();
        _current = contender;
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
        Payload == DataKind.String ? await _current!.GetAsync<string>(_pool.Next()) : await _current!.GetAsync<SampleRecord>(_pool.Next());
}
