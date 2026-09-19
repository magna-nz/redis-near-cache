using Microsoft.Extensions.Options;

namespace RedisNearCache;

/// <summary>
/// Validates <see cref="RedisNearCacheOptions"/> registered through <c>AddRedisNearCache</c>, collecting every
/// failure rather than stopping at the first so a caller sees the whole list at once.
/// </summary>
internal sealed class RedisNearCacheOptionsValidator : IValidateOptions<RedisNearCacheOptions>
{
    /// <inheritdoc/>
    public ValidateOptionsResult Validate(string? name, RedisNearCacheOptions options)
    {
        // Named options are not RedisNearCache's: AddRedisNearCache only ever configures the default instance.
        if (name is not null && name != Options.DefaultName) return ValidateOptionsResult.Skip;

        ArgumentNullException.ThrowIfNull(options);

        var failures = new List<string>();

        if (options.Configuration is null && string.IsNullOrWhiteSpace(options.ConnectionString))
        {
            failures.Add($"{nameof(RedisNearCacheOptions)}.{nameof(RedisNearCacheOptions.Configuration)} or " +
                         $"{nameof(RedisNearCacheOptions)}.{nameof(RedisNearCacheOptions.ConnectionString)} must be set.");
        }

        if (options.L1SizeLimit <= 0)
        {
            failures.Add($"{nameof(RedisNearCacheOptions)}.{nameof(RedisNearCacheOptions.L1SizeLimit)} must be greater than zero.");
        }

        if (options.L1SizeLimitBytes is <= 0)
        {
            failures.Add($"{nameof(RedisNearCacheOptions)}.{nameof(RedisNearCacheOptions.L1SizeLimitBytes)} must be greater than zero when set.");
        }

        if (options.L1MaxAge <= TimeSpan.Zero && options.L1MaxAge != Timeout.InfiniteTimeSpan)
        {
            failures.Add($"{nameof(RedisNearCacheOptions)}.{nameof(RedisNearCacheOptions.L1MaxAge)} must be greater than zero, or " +
                         $"{nameof(Timeout)}.{nameof(Timeout.InfiniteTimeSpan)} to disable it.");
        }

        if (string.IsNullOrEmpty(options.ClientNamePrefix) || options.ClientNamePrefix.Any(char.IsWhiteSpace))
        {
            failures.Add($"{nameof(RedisNearCacheOptions)}.{nameof(RedisNearCacheOptions.ClientNamePrefix)} must be non-empty and contain no whitespace (Redis CLIENT SETNAME rejects spaces).");
        }

        if (options.Serializer is null)
        {
            failures.Add($"{nameof(RedisNearCacheOptions)}.{nameof(RedisNearCacheOptions.Serializer)} must not be null.");
        }

        if (options.KeyPrefixes.Any(string.IsNullOrEmpty))
        {
            failures.Add($"{nameof(RedisNearCacheOptions)}.{nameof(RedisNearCacheOptions.KeyPrefixes)} must not contain a null or empty prefix.");
        }

        return failures.Count == 0 ? ValidateOptionsResult.Success : ValidateOptionsResult.Fail(failures);
    }
}
