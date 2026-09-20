using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Xunit;

namespace RedisNearCache.UnitTests;

/// <summary>
/// Option values that used to start a cache and then misbehave quietly, and one that used to refuse to start for no
/// reason. Validation runs for the default and for every named instance, so each rule is checked through both.
/// </summary>
public class OptionsValidationGapTests
{
    private static string? FailureFor(Action<RedisNearCacheOptions> configure)
    {
        var options = new RedisNearCacheOptions { ConnectionString = "localhost:6379" };
        configure(options);
        var result = RedisNearCacheOptionsValidator.ValidateRules(options);
        return result.Failed ? string.Join(" | ", result.Failures ?? []) : null;
    }

    /// <summary>
    /// A namespace goes in front of every key, so braces in it are a cluster hash tag and the whole cache lands in
    /// one slot - a hot shard, and a cross-slot failure for anything that batches. Documented on the property, and
    /// now refused.
    /// </summary>
    [Theory]
    [InlineData("{app}:")]
    [InlineData("app{1}:")]
    [InlineData("app}:")]
    [InlineData("{")]
    public void AKeyNamespaceContainingAHashTagIsRefused(string keyNamespace)
    {
        var failure = FailureFor(o => o.KeyNamespace = keyNamespace);
        Assert.NotNull(failure);
        Assert.Contains(nameof(RedisNearCacheOptions.KeyNamespace), failure, StringComparison.Ordinal);
        Assert.Contains("hash tag", failure, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("app:")]
    [InlineData("tenant-1/")]
    [InlineData("")]
    [InlineData(null)]
    public void AnOrdinaryKeyNamespaceIsAccepted(string? keyNamespace)
    {
        Assert.Null(FailureFor(o => o.KeyNamespace = keyNamespace));
    }

    [Theory]
    [InlineData(" app:")]
    [InlineData("app: ")]
    public void AKeyNamespaceWithSurroundingWhitespaceIsRefused(string keyNamespace)
    {
        var failure = FailureFor(o => o.KeyNamespace = keyNamespace);
        Assert.NotNull(failure);
        Assert.Contains("whitespace", failure, StringComparison.Ordinal);
    }

    /// <summary>
    /// A whitespace prefix matches nothing a caller would read, and in Broadcast mode it is sent as a
    /// <c>BCAST PREFIX</c> argument, arming the server for keys beginning with a space.
    /// </summary>
    [Theory]
    [InlineData(" ")]
    [InlineData("\t")]
    public void AWhitespaceKeyPrefixIsRefused(string prefix)
    {
        var failure = FailureFor(o => o.KeyPrefixes.Add(prefix));
        Assert.NotNull(failure);
        Assert.Contains(nameof(RedisNearCacheOptions.KeyPrefixes), failure, StringComparison.Ordinal);
    }

    [Fact]
    public void AnEmptyOrNullKeyPrefixIsStillRefused()
    {
        Assert.NotNull(FailureFor(o => o.KeyPrefixes.Add("")));
        Assert.NotNull(FailureFor(o => o.KeyPrefixes.Add(null!)));
    }

    /// <summary>
    /// With a byte budget set, <see cref="RedisNearCacheOptions.L1SizeLimit"/> is ignored (see <c>L1Cache</c>), so
    /// refusing to start over its value was a startup failure for a setting nothing reads.
    /// </summary>
    [Fact]
    public void AnIgnoredEntryCountLimitDoesNotFailValidation()
    {
        Assert.Null(FailureFor(o =>
        {
            o.L1SizeLimitBytes = 1024 * 1024;
            o.L1SizeLimit = 0;
        }));
    }

    [Fact]
    public void TheEntryCountLimitIsStillValidatedWhenItIsTheOneInForce()
    {
        var failure = FailureFor(o => o.L1SizeLimit = 0);
        Assert.NotNull(failure);
        Assert.Contains(nameof(RedisNearCacheOptions.L1SizeLimit), failure, StringComparison.Ordinal);
        Assert.NotNull(FailureFor(o => o.L1SizeLimitBytes = 0));
    }

    /// <summary>Every rule above must also be enforced for a named instance, through its own validator.</summary>
    [Fact]
    public void TheSameRulesApplyToANamedInstance()
    {
        var services = new ServiceCollection();
        services.AddKeyedRedisNearCache("tenant-a", o =>
        {
            o.ConnectionString = "localhost:6379";
            o.KeyNamespace = "{tenant-a}:";
        });
        using var provider = services.BuildServiceProvider();

        var ex = Assert.Throws<OptionsValidationException>(() => provider.GetRequiredKeyedService<IRedisNearCache>("tenant-a"));
        Assert.Contains("hash tag", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task AValidNamedInstanceStillResolves()
    {
        var services = new ServiceCollection();
        services.AddKeyedRedisNearCache("tenant-b", o =>
        {
            o.ConnectionString = "127.0.0.1:1,abortConnect=false";
            o.KeyNamespace = "tenant-b:";
            o.KeyPrefixes.Add("user:");
        });
        await using var provider = services.BuildServiceProvider();
        Assert.NotNull(provider.GetRequiredKeyedService<IRedisNearCache>("tenant-b"));
    }
}
