using System.Collections.Concurrent;

namespace RedisNearCache.Caching;

/// <summary>
/// Tracks keys with a Redis read in flight, so a reply for a key invalidated while the request was on the
/// wire can be discarded instead of being stored stale in L1.
///
/// A single monotonically increasing counter (<see cref="_version"/>) orders every <see cref="Begin"/> and
/// every <see cref="MarkInvalidated"/> call relative to one another, regardless of which key they concern.
/// <see cref="Begin"/> hands the caller the counter value at the moment the read started (the "token").
/// <see cref="MarkInvalidated"/> advances the counter and, for the key concerned, records the new value as
/// that key's "last invalidated at" version. <see cref="WasInvalidated"/> then answers "did an invalidation
/// for this key happen after this read started?" by comparing the recorded version to the token: since the
/// counter only ever increases, a recorded version greater than the token can only have been produced by a
/// <see cref="MarkInvalidated"/> call that happened after the corresponding <see cref="Begin"/> returned.
/// Two concurrent reads of the same key get their own tokens (usually different, since each call to
/// <see cref="Begin"/> also advances the counter) and each independently compares against the same per-key
/// recorded version, so both see an intervening invalidation.
///
/// Per-key state is reference-counted (<c>ActiveReaders</c>) and removed from the dictionary once the last
/// concurrent read for that key ends, so steady-state memory is proportional to keys currently in flight,
/// not to every key ever read.
/// </summary>
internal sealed class InFlightTracker
{
    private sealed class Entry
    {
        public int ActiveReaders;
        public long LastInvalidatedVersion;
        public bool Removed;
    }

    private readonly ConcurrentDictionary<string, Entry> _entries = new();
    private long _version;

    public long Begin(string key)
    {
        while (true)
        {
            var entry = _entries.GetOrAdd(key, static _ => new Entry());
            lock (entry)
            {
                if (entry.Removed)
                {
                    continue;
                }

                // Take the token only once the entry is registered and locked, so no MarkInvalidated can slip
                // between "token taken" and "entry visible" and be dropped.
                entry.ActiveReaders++;
                return Interlocked.Increment(ref _version);
            }
        }
    }

    public void End(string key, long token)
    {
        if (!_entries.TryGetValue(key, out var entry))
        {
            return;
        }

        lock (entry)
        {
            entry.ActiveReaders--;
            if (entry.ActiveReaders <= 0)
            {
                entry.Removed = true;
                ((ICollection<KeyValuePair<string, Entry>>)_entries).Remove(new KeyValuePair<string, Entry>(key, entry));
            }
        }
    }

    public void MarkInvalidated(string key)
    {
        long version = Interlocked.Increment(ref _version);

        if (!_entries.TryGetValue(key, out var entry))
        {
            return;
        }

        lock (entry)
        {
            if (!entry.Removed && version > entry.LastInvalidatedVersion)
            {
                entry.LastInvalidatedVersion = version;
            }
        }
    }

    public void MarkAllInvalidated()
    {
        long version = Interlocked.Increment(ref _version);

        foreach (var kvp in _entries)
        {
            var entry = kvp.Value;
            lock (entry)
            {
                if (!entry.Removed && version > entry.LastInvalidatedVersion)
                {
                    entry.LastInvalidatedVersion = version;
                }
            }
        }
    }

    public bool WasInvalidated(string key, long token)
    {
        if (!_entries.TryGetValue(key, out var entry))
        {
            return false;
        }

        lock (entry)
        {
            return entry.LastInvalidatedVersion > token;
        }
    }
}
