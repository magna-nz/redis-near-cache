# Draft comment for dotnet/runtime#129510

Not a new issue: the bug is already tracked as dotnet/runtime#129186 (regression from #103931 in 9.0), fixed on
`main` by #129215 (11.0 preview 6). The `release/10.0` backport, #129510, is open and labelled `blocked`, with the
maintainer "waiting for partner confirmation from validation that the issue is really solved" and inviting people to
test the fix on 11 and report back (comments of 2026-09-03 and 2026-09-10). This is that report.

Post on: https://github.com/dotnet/runtime/pull/129510

---

Independent validation, in case it helps unblock this backport.

I hit #129186 from a different direction: a Redis client-side cache (server-assisted invalidation) that uses
`MemoryCache` with a `SizeLimit` as its in-process store. Reads racing invalidations on hot keys are exactly
`Set` racing `Remove` on the same key, and under a write storm the store stopped accepting entries for good, with
nothing logged - the same signature described in the issue.

I reduced it to the console program below (no other dependencies) and ran it against the published packages, two
runs per version, 5 s each, on an Apple M-series machine (arm64, .NET SDK 10.0.400, `net10.0`). It has 6 threads
calling `Set` (with `Size = 1` and a 2 ms absolute expiration), 3 calling `Remove` and 3 calling `TryGetValue` over
12 keys, so all three removal paths race the replace path: `Remove`, the expiration scan, and the expired-entry
removal inside `TryGetValue`. Afterwards it removes every key and checks the size total, then stores one new entry.

| Microsoft.Extensions.Caching.Memory | `CurrentEstimatedSize` with 0 entries | a new entry is stored |
|---|---|---|
| 8.0.1 | 0 / 0 | yes |
| 9.0.20 | -5 / -7 | **no** |
| 10.0.12 | -5 / -6 | **no** |
| 11.0.0-preview.5.26302.115 | -8 / -7 | **no** |
| 11.0.0-preview.6.26359.118 (has #129215) | 0 / 0 | yes |
| 11.0.0-rc.1.26425.128 | 0 / 0 | yes |

Each run is roughly 50 million operations. So: 8.0 is clean, the regression is present from 9.0 through 10.0.12 and
11.0 preview 5, and the fix in #129215 holds under this load in preview 6 and rc.1, including with the expiration
scan and `TryGetValue` removals racing, not only `Remove`.

Two notes for anyone who needs a workaround on 9.0/10.0 until this ships, since "drop `SizeLimit`" is not always
an option:

- Serialising `Set` and `Remove` per key with a lock is **not** enough. With the locks in place the same program
  still ends at -2, because the expiration scan and `TryGetValue` remove entries outside the caller's lock.
- What does hold (0 across the same runs) is, under that per-key lock, calling `Remove(key)` immediately before
  `Set(key, ...)`. `SetEntry` then never finds a prior entry, so the speculative `newSize -= priorEntry.Size` is
  never taken, whoever else is removing. Lookups stay lock-free. `Clear()` also resets the total, since it swaps
  the `CoherentState`.

Repro (`dotnet run -c Release -p:MemVer=10.0.12`, with `<PackageReference Include="Microsoft.Extensions.Caching.Memory" Version="$(MemVer)" />`):

```csharp
using Microsoft.Extensions.Caching.Memory;

var cache = new MemoryCache(new MemoryCacheOptions
{
    SizeLimit = 10_000,
    TrackStatistics = true,
    ExpirationScanFrequency = TimeSpan.FromMilliseconds(50),
});
var keys = Enumerable.Range(0, 12).Select(i => "k" + i).ToArray();
var stop = DateTime.UtcNow.AddSeconds(5);

Task Run(Action<string> body) => Task.Run(() =>
{
    var random = new Random();
    while (DateTime.UtcNow < stop) body(keys[random.Next(keys.Length)]);
});

var workers = new List<Task>();
for (var i = 0; i < 6; i++)
    workers.Add(Run(key => cache.Set(key, new byte[8], new MemoryCacheEntryOptions
    {
        Size = 1,
        AbsoluteExpirationRelativeToNow = TimeSpan.FromMilliseconds(2),
    })));
for (var i = 0; i < 3; i++) workers.Add(Run(key => cache.Remove(key)));
for (var i = 0; i < 3; i++) workers.Add(Run(key => cache.TryGetValue(key, out _)));
await Task.WhenAll(workers);

foreach (var key in keys) cache.Remove(key);
await Task.Delay(300);

Console.WriteLine($"Count = {cache.Count}");                                                       // 0
Console.WriteLine($"CurrentEstimatedSize = {cache.GetCurrentStatistics()!.CurrentEstimatedSize}"); // expected 0
cache.Set("fresh", new byte[8], new MemoryCacheEntryOptions { Size = 1 });
Console.WriteLine($"a new entry is stored: {cache.TryGetValue("fresh", out _)}");                  // expected True
```

Given 10.0 is the LTS and the failure is silent (no exception, no log, the cache just stops caching until the
process restarts), it would be very good to see this in a 10.0.x patch.
