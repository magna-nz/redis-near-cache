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
        // Only the default instance, which is what AddRedisNearCache configures. A name given to
        // AddKeyedRedisNearCache has a validator of its own (NamedRedisNearCacheOptionsValidator); any other named
        // RedisNearCacheOptions an application keeps is none of this library's business.
        if (name is not null && name != Options.DefaultName) return ValidateOptionsResult.Skip;

        return ValidateRules(options);
    }

    /// <summary>The rules themselves, shared with <see cref="NamedRedisNearCacheOptionsValidator"/>.</summary>
    internal static ValidateOptionsResult ValidateRules(RedisNearCacheOptions options)
    {
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

/// <summary>
/// Validates the <see cref="RedisNearCacheOptions"/> registered under one name through <c>AddKeyedRedisNearCache</c>,
/// by the same rules as the default ones, and skips every other name.
/// </summary>
/// <remarks>
/// A type of its own on purpose. <c>AddRedisNearCache</c> adds the default validator with <c>TryAddEnumerable</c>,
/// which skips the add when a descriptor with the same service type AND implementation type is already present. Had
/// this been the same class, a named registration made first would have cost the default instance its validation,
/// silently.
/// </remarks>
internal sealed class NamedRedisNearCacheOptionsValidator(string name) : IValidateOptions<RedisNearCacheOptions>
{
    /// <summary>The options name this instance validates.</summary>
    internal string Name => name;

    /// <inheritdoc/>
    public ValidateOptionsResult Validate(string? optionsName, RedisNearCacheOptions options) =>
        (optionsName ?? Options.DefaultName) != name
            ? ValidateOptionsResult.Skip
            : RedisNearCacheOptionsValidator.ValidateRules(options);
}
