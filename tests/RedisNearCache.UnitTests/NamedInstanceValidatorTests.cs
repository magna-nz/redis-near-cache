using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Xunit;

namespace RedisNearCache.UnitTests;

/// <summary>
/// <see cref="RedisNearCacheOptionsValidator"/> now validates exactly ONE options name - the default one when it
/// was built by the container (which is what <c>AddRedisNearCache</c> registers), or the name
/// <c>AddKeyedRedisNearCache</c> asked for. The rule that has to survive is the one that was there before: options
/// an application keeps under a name of its own are still skipped, by every validator RedisNearCache registers.
/// </summary>
public class NamedInstanceValidatorTests
{
    private static RedisNearCacheOptions Invalid() => new(); // no Configuration and no ConnectionString

    [Fact]
    public void ForNameValidatesThatNameAndSkipsTheDefaultOne()
    {
        var validator = new NamedRedisNearCacheOptionsValidator("x");

        Assert.True(validator.Validate("x", Invalid()).Failed);
        Assert.True(validator.Validate(Options.DefaultName, Invalid()).Skipped);
        Assert.True(validator.Validate(null, Invalid()).Skipped);
    }

    [Fact]
    public void ForNameSkipsEveryOtherName()
    {
        var validator = new NamedRedisNearCacheOptionsValidator("x");

        Assert.True(validator.Validate("y", Invalid()).Skipped);
        Assert.True(validator.Validate("X", Invalid()).Skipped); // names are compared exactly, as options names are
    }

    [Fact]
    public void ForNameReportsTheNameItValidates()
    {
        Assert.Equal("x", new NamedRedisNearCacheOptionsValidator("x").Name);
    }

    /// <summary>The parameterless validator, the only one that existed before, is unchanged.</summary>
    [Fact]
    public void TheDefaultValidatorStillValidatesTheDefaultNameAndSkipsEveryOther()
    {
        var validator = new RedisNearCacheOptionsValidator();

        Assert.True(validator.Validate(null, Invalid()).Failed);
        Assert.True(validator.Validate(Options.DefaultName, Invalid()).Failed);
        Assert.True(validator.Validate("x", Invalid()).Skipped);
    }

    /// <summary>A named validator applies the same rules to its own name, not a weaker set.</summary>
    [Fact]
    public void ForNameAppliesTheSameRules()
    {
        var validator = new NamedRedisNearCacheOptionsValidator("x");
        var options = new RedisNearCacheOptions { ConnectionString = "localhost:6379", L1SizeLimit = 0 };

        var result = validator.Validate("x", options);

        Assert.True(result.Failed);
        Assert.Contains(result.Failures, f => f.Contains(nameof(RedisNearCacheOptions.L1SizeLimit)));
    }

    // --- how they are registered ------------------------------------------------------------------------------

    private static List<NamedRedisNearCacheOptionsValidator> NamedValidators(IServiceCollection services) =>
        services
            .Where(d => d.ServiceType == typeof(IValidateOptions<RedisNearCacheOptions>))
            .Select(d => d.ImplementationInstance)
            .OfType<NamedRedisNearCacheOptionsValidator>()
            .ToList();

    [Fact]
    public void AddingOneNameTwiceRegistersOneValidatorForIt()
    {
        var services = new ServiceCollection();
        services.AddKeyedRedisNearCache("a", "localhost:6379");
        services.AddKeyedRedisNearCache("a", "localhost:6379");

        Assert.Equal(new[] { "a" }, NamedValidators(services).Select(v => v.Name).ToArray());
    }

    [Fact]
    public void TwoNamesGetOneValidatorEach()
    {
        var services = new ServiceCollection();
        services.AddKeyedRedisNearCache("a", "localhost:6379");
        services.AddKeyedRedisNearCache("b", "localhost:6379");

        Assert.Equal(new[] { "a", "b" }, NamedValidators(services).Select(v => v.Name).OrderBy(n => n, StringComparer.Ordinal).ToArray());
    }

    /// <summary>
    /// The default registration's validator is still the type-registered one <c>AddRedisNearCache</c> always added
    /// (<see cref="RedisNearCacheOptionsValidatorTests.AddingTwiceRegistersTheValidatorOnlyOnce"/> counts exactly
    /// those), and a named registration adds an instance alongside it rather than displacing it.
    /// </summary>
    [Fact]
    public void ANamedRegistrationLeavesTheDefaultValidatorRegistrationAlone()
    {
        var services = new ServiceCollection();
        services.AddRedisNearCache("localhost:6379");
        services.AddKeyedRedisNearCache("a", "localhost:6379");

        var byType = services.Count(d =>
            d.ServiceType == typeof(IValidateOptions<RedisNearCacheOptions>) &&
            d.ImplementationType == typeof(RedisNearCacheOptionsValidator));

        Assert.Equal(1, byType);
        Assert.Equal(new[] { "a" }, NamedValidators(services).Select(v => v.Name).ToArray());
    }
}
