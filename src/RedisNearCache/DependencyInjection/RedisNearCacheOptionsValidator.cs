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

        // Only when it is the limit in force: with L1SizeLimitBytes set, L1SizeLimit is ignored (see L1Cache), and
        // failing on a value nothing reads would be a startup failure for nothing.
        if (options.L1SizeLimitBytes is null && options.L1SizeLimit <= 0)
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

        // Whitespace as well as empty: a prefix of " " matches nothing a caller would ever read, and in Broadcast mode
        // it goes out as a BCAST PREFIX argument, arming the server for keys that begin with a space.
        if (options.KeyPrefixes.Any(string.IsNullOrWhiteSpace))
        {
            failures.Add($"{nameof(RedisNearCacheOptions)}.{nameof(RedisNearCacheOptions.KeyPrefixes)} must not contain a null, empty or whitespace prefix.");
        }

        // A namespace is concatenated in front of every key, so braces in it are a cluster hash tag and every key the
        // cache touches would hash to one slot - documented on the property, and cheap to refuse outright.
        if (options.KeyNamespace is { Length: > 0 } keyNamespace && (keyNamespace.Contains('{') || keyNamespace.Contains('}')))
        {
            failures.Add($"{nameof(RedisNearCacheOptions)}.{nameof(RedisNearCacheOptions.KeyNamespace)} must not contain '{{' or '}}': on a cluster that is a hash tag, and every key would land in one slot.");
        }

        if (options.KeyNamespace is { Length: > 0 } padded && padded.Trim().Length != padded.Length)
        {
            failures.Add($"{nameof(RedisNearCacheOptions)}.{nameof(RedisNearCacheOptions.KeyNamespace)} must not begin or end with whitespace: it is concatenated in front of every key.");
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
