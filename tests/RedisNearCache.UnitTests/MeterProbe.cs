using System.Diagnostics.Metrics;

namespace RedisNearCache.UnitTests;

/// <summary>
/// One round of observable measurements from the <see cref="RedisNearCacheStatistics.MeterName"/> meter, for one
/// cache instance.
/// </summary>
/// <param name="Longs">
/// Every <see cref="long"/> instrument, keyed by name, SUMMED over its tag sets. Summing is what makes a
/// multi-measurement instrument (<c>redisnearcache.rearms</c>, <c>redisnearcache.flushes</c>: one measurement per
/// <c>reason</c>) read as the single total a dashboard that ignores the tag would show. Every other instrument emits
/// exactly one measurement, so summing leaves it alone.
/// </param>
/// <param name="Doubles">The same for <see cref="double"/> instruments; only <c>redisnearcache.pass_through.seconds</c> is one.</param>
/// <param name="ByReason">
/// The <see cref="long"/> measurements keyed by instrument name AND the value of the <c>reason</c> tag (null when the
/// instrument does not carry one), which is what a per-reason assertion needs: keying on the name alone would keep
/// only whichever reason happened to be emitted last.
/// </param>
internal sealed record MeterSnapshot(
    Dictionary<string, long> Longs,
    Dictionary<string, double> Doubles,
    Dictionary<(string Instrument, string? Reason), long> ByReason)
{
    /// <summary>Every instrument name that published something, whatever its value type.</summary>
    public IReadOnlyCollection<string> Names => Longs.Keys.Concat(Doubles.Keys).ToList();

    public long Reason(string instrument, Enum reason) => ByReason.TryGetValue((instrument, reason.ToString()), out var v) ? v : -1;
}

/// <summary>
/// Collects the meter for exactly one cache instance. Every reading filters on that instance's
/// <c>rnc.client_name</c> tag: other test classes run in parallel in the same process and publish to a meter of the
/// same name, so an unfiltered assertion is flaky.
/// </summary>
internal static class MeterProbe
{
    public const string ClientNameTag = "rnc.client_name";
    public const string ReasonTag = "reason";

    public static MeterSnapshot Collect(string clientName)
    {
        var longs = new Dictionary<string, long>();
        var doubles = new Dictionary<string, double>();
        var byReason = new Dictionary<(string, string?), long>();

        using var listener = new MeterListener();
        listener.InstrumentPublished = (instrument, l) =>
        {
            if (instrument.Meter.Name == RedisNearCacheStatistics.MeterName) l.EnableMeasurementEvents(instrument);
        };
        listener.SetMeasurementEventCallback<long>((instrument, value, tags, _) =>
        {
            if (!Mine(tags, clientName)) return;
            longs[instrument.Name] = longs.TryGetValue(instrument.Name, out var running) ? running + value : value;
            byReason[(instrument.Name, ReasonOf(tags))] = value;
        });
        listener.SetMeasurementEventCallback<double>((instrument, value, tags, _) =>
        {
            if (!Mine(tags, clientName)) return;
            doubles[instrument.Name] = doubles.TryGetValue(instrument.Name, out var running) ? running + value : value;
        });
        listener.Start();
        listener.RecordObservableInstruments();
        return new MeterSnapshot(longs, doubles, byReason);
    }

    private static bool Mine(ReadOnlySpan<KeyValuePair<string, object?>> tags, string clientName)
    {
        foreach (var tag in tags)
        {
            if (tag.Key == ClientNameTag && (string?)tag.Value == clientName) return true;
        }

        return false;
    }

    private static string? ReasonOf(ReadOnlySpan<KeyValuePair<string, object?>> tags)
    {
        foreach (var tag in tags)
        {
            if (tag.Key == ReasonTag) return (string?)tag.Value;
        }

        return null;
    }
}
