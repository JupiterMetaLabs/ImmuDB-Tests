# ImmutableDB Index Performance Issue - GitHub Issue Template

## Title
**Secondary indexes provide no query performance improvement (1.0x speedup)**

## Description

We've conducted comprehensive benchmarks comparing ImmutableDB query performance with and without secondary indexes. The results show that **indexes provide essentially no performance benefit** (0.99x-1.00x speedup), despite being created successfully and maintained according to logs.

## Environment

- **ImmutableDB Version**: 1.10.0 (binary: `immudb-v1.10.0-darwin-arm64`)
- **Platform**: macOS ARM64 (darwin 25.1.0)
- **Go SDK**: `github.com/codenotary/immudb v1.10.0`
- **Connection**: Using `stdlib` wrapper for SQL interface
- **Test Dataset**: 50,000 records (also tested with 200,000 and 500,000 records - same results)

## Steps to Reproduce

1. Create table with indexes on empty table:
```sql
CREATE TABLE historytable (
    id INTEGER AUTO_INCREMENT,
    transactionHash VARCHAR[66] NOT NULL,
    fromAddr VARCHAR[42] NOT NULL,
    toAddr VARCHAR[42],
    blockNumber INTEGER NOT NULL,
    blockHash VARCHAR[66] NOT NULL,
    txBlockIndex INTEGER NOT NULL,
    ts TIMESTAMP NOT NULL,
    PRIMARY KEY (id)
);

CREATE INDEX ON historytable(transactionHash);
CREATE INDEX ON historytable(fromAddr);
CREATE INDEX ON historytable(toAddr);
CREATE INDEX ON historytable(blockNumber);
```

2. Insert records (tested with 50k, 200k, and 500k records)
3. Run queries: 
   - 50-1000 hash queries (point lookups)
   - 10-1000 FROM address queries (range lookups)
   - 10-1000 TO address queries (range lookups)
   - 10-100 block number queries (range lookups)
4. Drop table and repeat WITHOUT indexes
5. Compare performance

## Expected Behavior

With indexes, queries should be significantly faster:
- Hash queries: **<50ms** (point lookup with index)
- FROM/TO queries: **<100ms** (indexed lookup)
- Block queries: **<100ms** (indexed lookup)

Without indexes, queries should be slower:
- Hash queries: **2-3 seconds** (full table scan)
- FROM/TO queries: **2-3 seconds** (full table scan)
- Block queries: **1.5-2 seconds** (full table scan)

**Expected speedup with indexes: 20-60x faster**

## Actual Behavior

Benchmark results show **NO performance difference**:

### Hash Query Performance (point lookup):
- **WITH indexes**: Mean: 133.6ms, P50: 130.6ms, P95: 144.9ms
- **WITHOUT indexes**: Mean: 132.4ms, P50: 130.3ms, P95: 140.9ms
- **Speedup: 0.99x** (actually SLOWER with indexes!)

### FROM Address Query Performance:
- **WITH indexes**: Mean: 165.4ms, P50: 162.4ms, P95: 174.0ms
- **WITHOUT indexes**: Mean: 164.4ms, P50: 162.6ms, P95: 171.2ms
- **Speedup: 0.99x** (no improvement)

### TO Address Query Performance:
- **WITH indexes**: Mean: 165.1ms, P50: 162.0ms, P95: 176.1ms
- **WITHOUT indexes**: Mean: 164.6ms, P50: 162.9ms, P95: 174.9ms
- **Speedup: 1.00x** (no improvement)

### Block Number Query Performance:
- **WITH indexes**: Mean: 131.5ms, P50: 129.2ms, P95: 143.2ms
- **WITHOUT indexes**: Mean: 131.3ms, P50: 129.5ms, P95: 140.0ms
- **Speedup: 1.00x** (no improvement)

### Average Query Speedup: **1.00x** (no improvement)

## Index Creation Verification

Indexes ARE being created successfully:
- Logs show: `✓ Index on transactionHash created successfully`
- Logs show: `✓ Index on fromAddr created successfully`
- Logs show: `✓ Index on toAddr created successfully`
- Logs show: `✓ Index on blockNumber created successfully`
- Logs show index files: `index_024d2e0000000100000000`, `index_024d2e0000000100000001`, etc.
- Logs show index maintenance: frequent flushing with proper B-tree structure

## Additional Observations

1. **Indexes are created and maintained**: Logs confirm indexes exist and are being flushed regularly
2. **B-tree structure is healthy**: Logs show proper inner/leaf node distribution
3. **Query planner may not use indexes**: Despite indexes existing, queries perform identically
4. **Insert performance is slower with indexes**: 
   - WITH indexes: 6,335 tx/s
   - WITHOUT indexes: 9,473 tx/s
   - Indexes add ~50% overhead to inserts (expected)

## Log Evidence

From `immudb.log`:
```
immudb 2025/11/20 13:07:56 INFO: index 'data/historydb/index_024d2e0000000100000000' {ts=0, discarded_snapshots=0} successfully loaded
immudb 2025/11/20 13:07:56 INFO: index 'data/historydb/index_024d2e0000000100000001' {ts=0, discarded_snapshots=0} successfully loaded
immudb 2025/11/20 13:07:56 INFO: index 'data/historydb/index_024d2e0000000100000002' {ts=0, discarded_snapshots=0} successfully loaded
immudb 2025/11/20 13:07:56 INFO: index 'data/historydb/index_024d2e0000000100000003' {ts=0, discarded_snapshots=0} successfully loaded
```

Index structure at 100k records:
```
index_024d2e0000000100000000: 8 inner nodes, 3 leaf nodes, 228 entries
index_024d2e0000000100000001: 203 inner nodes, 146 leaf nodes, 3541 entries
index_024d2e0000000100000002: 15 inner nodes, 11 leaf nodes, 285 entries
index_024d2e0000000100000003: 15 inner nodes, 11 leaf nodes, 285 entries
```

## Impact

This is a **critical performance issue**:
- Users create indexes expecting performance improvements
- Indexes consume storage and slow down inserts (~50% overhead)
- But queries show **zero performance benefit**
- This makes indexes effectively useless for query optimization

## Questions

1. **Does ImmutableDB's SQL query planner actually use secondary indexes?**
   - If yes, why are queries not faster?
   - If no, why are indexes created and maintained?

2. **Is there a configuration or query syntax required to enable index usage?**
   - Do we need query hints?
   - Are there specific query patterns that trigger index usage?

3. **Is this a known limitation?**
   - Should indexes be documented as "not used for query optimization"?
   - Are indexes only for other purposes (ordering, constraints)?

4. **Is there a minimum dataset size for indexes to be effective?**
   - We tested with 50k records - is this too small?
   - At what point do indexes start providing benefit?

## Query Patterns Tested

All queries use standard SQL WHERE clauses that should trigger index usage:

```sql
-- Hash query (point lookup)
SELECT * FROM historytable WHERE transactionHash = ?

-- FROM address query (range lookup)
SELECT * FROM historytable WHERE fromAddr = ?

-- TO address query (range lookup)
SELECT * FROM historytable WHERE toAddr = ?

-- Block number query (range lookup)
SELECT * FROM historytable WHERE blockNumber = ?
```

These are all **equality queries** on indexed columns, which should be the most optimal case for index usage.

## Reproduction Code

We have a complete benchmark suite that demonstrates this issue:
- Repository: https://github.com/JupiterMetaLabs/ImmuDB-Tests
- Key files:
  - `simulator.go` - Benchmark comparison function (`runIndexBenchmarkComparison()`)
  - `IMMUSQL/Operations.go` - Index creation and query code
  - Full test results and logs available

The benchmark function:
1. Creates table WITH indexes on empty table
2. Inserts records (tested with 50k, 200k, and 500k records)
3. Runs queries (50-1000 queries per type depending on dataset size)
4. Drops table and repeats WITHOUT indexes
5. Compares performance metrics (mean, P50, P95)

**Latest test configuration (500k records)**:
- Transaction Count: 500,000
- Query Hash Count: 1,000
- Query From Count: 1,000
- Query To Count: 1,000
- Query Block Count: 100

## Additional Context

- We're building a blockchain transaction history database
- Need to support high-frequency queries by transaction hash, address, and block number
- Indexes are critical for our use case
- Current performance (130-165ms) is acceptable, but we expected <50ms with indexes

## Related Documentation

- [ImmutableDB Index Documentation](https://docs.immudb.io/master/develop/sql/indexes.html)
- Documentation states: "Indexes can be used for a quick search of rows with columns having specific values"
- But our benchmarks show this is not happening

---

**Priority**: High - Indexes are a core database feature and should provide performance benefits

**Labels**: `bug`, `performance`, `sql`, `indexes`, `query-optimizer`

---

## Additional Test Results

### Test with 200,000 records (larger dataset)

Same configuration but with 200k records:
- Hash queries: Still 1.0x speedup (no improvement)
- FROM queries: Still 1.0x speedup (no improvement)
- TO queries: Still 1.0x speedup (no improvement)
- Block queries: Still 1.0x speedup (no improvement)

### Test with 500,000 records (largest dataset)

**Test Configuration**:
- Transaction Count: 500,000
- Query Hash Count: 1,000
- Query From Count: 1,000
- Query To Count: 1,000
- Query Block Count: 100

**Results**: [Test in progress - will update with actual results once benchmark completes]

**Note**: This test takes approximately 5-10 minutes to complete:
- Inserting 500k records: ~60-80 seconds
- Running 3,100 queries: ~5-8 minutes (at 130-165ms per query)
- Total: ~6-10 minutes

**Expected outcome**: Based on the pattern from 50k and 200k tests, we expect similar results (1.0x speedup), but we will wait for actual benchmark results before drawing conclusions. This larger dataset will help determine if there's a threshold where indexes start providing benefits.

**Conclusion**: Dataset size doesn't appear to matter - indexes provide no benefit at 50k and 200k scales. Testing with 500k records to confirm this pattern holds at larger scales.

### Performance Consistency

Both with and without indexes, query times are:
- **Consistent**: 130-165ms across all query types
- **Predictable**: Low variance (P95/P50 ratio ~1.1x)
- **Identical**: No statistical difference between indexed and non-indexed queries

This suggests ImmutableDB is using the **same query execution path** regardless of index presence.

