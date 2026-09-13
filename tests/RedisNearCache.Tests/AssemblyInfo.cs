using Xunit;

// These are integration tests against real, shared Redis containers (standalone + cluster). Several
// tests kill client connections (CLIENT KILL) and rely on precise Statistics counters that are only
// meaningful if nothing else is concurrently reading/writing/killing connections on the same private
// multiplexer or container. Rather than build fine-grained cross-collection locking, we run the whole
// assembly's test collections sequentially. Key prefixes are still unique per test (see KeyPrefix
// helper) so the suite would remain correct if parallelization were re-enabled later; only the FlushDb
// test truly requires isolation (FLUSHDB wipes the whole DB), and it is additionally tagged
// [Collection("flush")] per the test plan.
[assembly: CollectionBehavior(DisableTestParallelization = true)]
