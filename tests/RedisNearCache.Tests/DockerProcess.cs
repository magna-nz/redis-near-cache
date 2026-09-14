using System.Diagnostics;

namespace RedisNearCache.Tests;

/// <summary>
/// The asynchronous <c>docker</c> invocation behind <see cref="RedisCli.RunAsync"/>, <see cref="Chaos.DockerExec.RunAsync"/>
/// and <see cref="Resilience.ResilienceSupport.DockerAsync"/>. Their synchronous forms hold a thread-pool thread for
/// the whole ~80 ms of every <c>docker exec</c>; tests that poll through docker while the private multiplexer is
/// reconnecting (when StackExchange.Redis is itself holding pool threads, see <see cref="ThreadPoolSetup"/>) use these.
/// </summary>
internal static class DockerProcess
{
    public static readonly TimeSpan DefaultTimeout = TimeSpan.FromMinutes(2);

    public static async Task<(int ExitCode, string StdOut, string StdErr)> RunAsync(IEnumerable<string> args, TimeSpan? timeout = null)
    {
        var psi = new ProcessStartInfo("docker")
        {
            RedirectStandardOutput = true,
            RedirectStandardError = true,
        };
        foreach (var a in args) psi.ArgumentList.Add(a);

        var limit = timeout ?? DefaultTimeout;
        using var cts = new CancellationTokenSource(limit);
        using var p = Process.Start(psi)!;
        // Both pipes are drained concurrently: reading one to the end first can deadlock once the other one fills.
        var stdout = p.StandardOutput.ReadToEndAsync(cts.Token);
        var stderr = p.StandardError.ReadToEndAsync(cts.Token);
        try
        {
            await p.WaitForExitAsync(cts.Token);
            return (p.ExitCode, (await stdout).Trim(), (await stderr).Trim());
        }
        catch (OperationCanceledException) when (cts.IsCancellationRequested)
        {
            // Best effort: the process may have exited meanwhile, or the tree may not be killable; the timeout is the report.
            try { p.Kill(entireProcessTree: true); }
            catch (Exception ex) when (ex is InvalidOperationException or System.ComponentModel.Win32Exception or AggregateException) { }
            throw new TimeoutException($"docker {string.Join(' ', psi.ArgumentList)} did not exit within {limit}.");
        }
    }
}
