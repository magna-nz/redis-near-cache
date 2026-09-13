namespace RedisNearCache.Tests;

/// <summary>Deadline polling helpers. No fixed sleeps: every wait for an async server-side effect
/// (invalidation, re-arm, reconnect) polls a condition until it is true or a deadline passes.</summary>
internal static class Poll
{
    public static readonly TimeSpan DefaultTimeout = TimeSpan.FromSeconds(3);
    public static readonly TimeSpan DefaultStep = TimeSpan.FromMilliseconds(20);

    public static async Task<bool> UntilAsync(Func<bool> condition, TimeSpan? timeout = null, TimeSpan? step = null)
    {
        var deadline = DateTime.UtcNow + (timeout ?? DefaultTimeout);
        var interval = step ?? DefaultStep;
        while (true)
        {
            if (condition()) return true;
            if (DateTime.UtcNow >= deadline) return condition();
            await Task.Delay(interval);
        }
    }

    public static async Task<bool> UntilAsync(Func<Task<bool>> condition, TimeSpan? timeout = null, TimeSpan? step = null)
    {
        var deadline = DateTime.UtcNow + (timeout ?? DefaultTimeout);
        var interval = step ?? DefaultStep;
        while (true)
        {
            if (await condition().ConfigureAwait(false)) return true;
            if (DateTime.UtcNow >= deadline) return await condition().ConfigureAwait(false);
            await Task.Delay(interval);
        }
    }
}
