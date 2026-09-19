using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Xunit;

namespace RedisNearCache.UnitTests;

/// <summary>
/// <see cref="RedisNearCacheOptionsValidator"/> (registered by <c>AddRedisNearCache</c>) and DI wiring around it:
/// no Redis, so failures are asserted purely from <see cref="ValidateOptionsResult"/> and
/// <see cref="IOptions{TOptions}"/> resolution.
/// </summary>
public class RedisNearCacheOptionsValidatorTests
{
    private static readonly RedisNearCacheOptionsValidator Validator = new();

    private static RedisNearCacheOptions Valid() => new() { ConnectionString = "localhost:6379" };

    [Fact]
    public void DefaultOptionsWithConnectionStringSucceed()
    {
        var result = Validator.Validate(null, Valid());
        Assert.False(result.Failed);
    }

    [Fact]
    public void ConfigurationWithoutConnectionStringSucceeds()
    {
        var options = new RedisNearCacheOptions { Configuration = StackExchange.Redis.ConfigurationOptions.Parse("localhost:6379") };
        var result = Validator.Validate(null, options);
        Assert.False(result.Failed);
    }

    [Fact]
    public void NeitherConfigurationNorConnectionStringFails()
    {
        var result = Validator.Validate(null, new RedisNearCacheOptions());
        Assert.True(result.Failed);
        Assert.Contains(result.Failures, f => f.Contains(nameof(RedisNearCacheOptions.Configuration)) && f.Contains(nameof(RedisNearCacheOptions.ConnectionString)));
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public void NonPositiveL1SizeLimitFails(long value)
    {
        var options = Valid();
        options.L1SizeLimit = value;
        var result = Validator.Validate(null, options);
        Assert.True(result.Failed);
        Assert.Contains(result.Failures, f => f.Contains(nameof(RedisNearCacheOptions.L1SizeLimit)));
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public void NonPositiveL1SizeLimitBytesFails(long value)
    {
        var options = Valid();
        options.L1SizeLimitBytes = value;
        var result = Validator.Validate(null, options);
        Assert.True(result.Failed);
        Assert.Contains(result.Failures, f => f.Contains(nameof(RedisNearCacheOptions.L1SizeLimitBytes)));
    }

    [Fact]
    public void NullL1SizeLimitBytesIsFine()
    {
        var options = Valid();
        options.L1SizeLimitBytes = null;
        Assert.False(Validator.Validate(null, options).Failed);
    }

    [Fact]
    public void ZeroL1MaxAgeFails()
    {
        var options = Valid();
        options.L1MaxAge = TimeSpan.Zero;
        var result = Validator.Validate(null, options);
        Assert.True(result.Failed);
        Assert.Contains(result.Failures, f => f.Contains(nameof(RedisNearCacheOptions.L1MaxAge)));
    }

    [Fact]
    public void NegativeL1MaxAgeOtherThanInfiniteFails()
    {
        var options = Valid();
        options.L1MaxAge = TimeSpan.FromSeconds(-5);
        var result = Validator.Validate(null, options);
        Assert.True(result.Failed);
        Assert.Contains(result.Failures, f => f.Contains(nameof(RedisNearCacheOptions.L1MaxAge)));
    }

    [Fact]
    public void InfiniteL1MaxAgePasses()
    {
        var options = Valid();
        options.L1MaxAge = Timeout.InfiniteTimeSpan;
        Assert.False(Validator.Validate(null, options).Failed);
    }

    [Theory]
    [InlineData("")]
    [InlineData(" ")]
    [InlineData("has space")]
    [InlineData("tab\ted")]
    public void BadClientNamePrefixFails(string prefix)
    {
        var options = Valid();
        options.ClientNamePrefix = prefix;
        var result = Validator.Validate(null, options);
        Assert.True(result.Failed);
        Assert.Contains(result.Failures, f => f.Contains(nameof(RedisNearCacheOptions.ClientNamePrefix)));
    }

    [Fact]
    public void NullSerializerFails()
    {
        var options = Valid();
        options.Serializer = null!;
        var result = Validator.Validate(null, options);
        Assert.True(result.Failed);
        Assert.Contains(result.Failures, f => f.Contains(nameof(RedisNearCacheOptions.Serializer)));
    }

    [Fact]
    public void EmptyKeyPrefixesListPasses()
    {
        var options = Valid();
        Assert.Empty(options.KeyPrefixes);
        Assert.False(Validator.Validate(null, options).Failed);
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    public void NullOrEmptyKeyPrefixEntryFails(string? badPrefix)
    {
        var options = Valid();
        options.KeyPrefixes.Add(badPrefix!);
        var result = Validator.Validate(null, options);
        Assert.True(result.Failed);
        Assert.Contains(result.Failures, f => f.Contains(nameof(RedisNearCacheOptions.KeyPrefixes)));
    }

    [Fact]
    public void SeveralBadValuesAreAllReported()
    {
        var options = new RedisNearCacheOptions
        {
            L1SizeLimit = 0,
            L1MaxAge = TimeSpan.Zero,
            ClientNamePrefix = "has space",
        };
        var result = Validator.Validate(null, options);
        Assert.True(result.Failed);
        // Missing Configuration/ConnectionString, L1SizeLimit, L1MaxAge and ClientNamePrefix: four failures.
        Assert.Equal(4, result.Failures.Count());
    }

    [Theory]
    [InlineData("some-other-name")]
    public void NonDefaultNamedOptionsAreSkipped(string name)
    {
        // An instance that would fail every rule, but under a name RedisNearCache never configures.
        var result = Validator.Validate(name, new RedisNearCacheOptions());
        Assert.True(IsSkip(result));
    }

    [Fact]
    public void DefaultNameIsValidatedNormally()
    {
        var result = Validator.Validate(Options.DefaultName, new RedisNearCacheOptions());
        Assert.True(result.Failed);
    }

    private static bool IsSkip(ValidateOptionsResult result) => result.Skipped;

    [Fact]
    public void ResolvingOptionsWithoutConfigurationThrowsOptionsValidationException()
    {
        var services = new ServiceCollection();
        services.AddRedisNearCache(_ => { });
        var provider = services.BuildServiceProvider();

        Assert.Throws<OptionsValidationException>(() => provider.GetRequiredService<IOptions<RedisNearCacheOptions>>().Value);
    }

    [Fact]
    public void AddingTwiceRegistersTheValidatorOnlyOnce()
    {
        var services = new ServiceCollection();
        services.AddRedisNearCache("localhost:6379");
        services.AddRedisNearCache("localhost:6379");

        var validatorDescriptors = services.Where(d =>
            d.ServiceType == typeof(IValidateOptions<RedisNearCacheOptions>) &&
            d.ImplementationType == typeof(RedisNearCacheOptionsValidator)).ToList();

        Assert.Single(validatorDescriptors);
    }
}
