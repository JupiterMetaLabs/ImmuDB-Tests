package IMMUSQL

import (
    "context"
    "fmt"
    "time"
)

// CompareOrderByIndexTest runs each diagnostic query twice (with and without ORDER BY on the filtered column),
// measures average execution time over several iterations and prints a comparison for each index-tested column.
func (t *TableOps) CompareOrderByIndexTest(ctx context.Context, tableName string) error {
    fmt.Println("\n=== Compare ORDER BY effect on index usage ===")

    // diagnostic queries and test values (reuse values from TestIndexPerformance)
    testHash := "0x90b01ec0ed76601314559f16eefb873bbf1a0a145f805358d0c377944593403c"
    testFrom := "0x742d35Cc6634C0532925a3b844Bc9e7595f0bEb0"
    testTo := testFrom
    testBlockNumber := 51 // use an unlikely block number to measure lookup time (works for COUNT)

    // each item: name, sql without ORDER BY, sql with ORDER BY, arg
    type qitem struct {
        name    string
        noOrder string
        withOrd string
        arg     interface{}
    }
    qs := []qitem{
        {
            name:    "transactionHash",
            noOrder: fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE transactionHash = ?", tableName),
            withOrd: fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE transactionHash = ? ORDER BY transactionHash", tableName),
            arg:     testHash,
        },
        {
            name:    "fromAddr",
            noOrder: fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE fromAddr = ?", tableName),
            withOrd: fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE fromAddr = ? ORDER BY fromAddr", tableName),
            arg:     testFrom,
        },
        {
            name:    "toAddr",
            noOrder: fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE toAddr = ?", tableName),
            withOrd: fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE toAddr = ? ORDER BY toAddr", tableName),
            arg:     testTo,
        },
        {
            name:    "blockNumber",
            noOrder: fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE blockNumber = ?", tableName),
            withOrd: fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE blockNumber = ? ORDER BY blockNumber", tableName),
            arg:     testBlockNumber,
        },
    }

    iterations := 5

    for _, q := range qs {
        var totalNo time.Duration
        var totalWith time.Duration
        var lastCountNo int
        var lastCountWith int

        for i := 0; i < iterations; i++ {
            // without ORDER BY
            start := time.Now()
            var cntNo int
            err := t.DB.QueryRowContext(ctx, q.noOrder, q.arg).Scan(&cntNo)
            durNo := time.Since(start)
            if err != nil {
                return fmt.Errorf("query (%s) no-order failed: %w", q.name, err)
            }
            totalNo += durNo
            lastCountNo = cntNo

            // with ORDER BY
            start = time.Now()
            var cntWith int
            err = t.DB.QueryRowContext(ctx, q.withOrd, q.arg).Scan(&cntWith)
            durWith := time.Since(start)
            if err != nil {
                return fmt.Errorf("query (%s) with-order failed: %w", q.name, err)
            }
            totalWith += durWith
            lastCountWith = cntWith
        }

        avgNo := totalNo / time.Duration(iterations)
        avgWith := totalWith / time.Duration(iterations)

        fmt.Printf("\nIndex test: %s\n", q.name)
        fmt.Printf("  Result counts -> without ORDER BY: %d, with ORDER BY: %d\n", lastCountNo, lastCountWith)
        fmt.Printf("  Avg time (without ORDER BY): %v\n", avgNo)
        fmt.Printf("  Avg time (with    ORDER BY): %v\n", avgWith)

        if avgWith < avgNo {
            fmt.Printf("  ✓ ORDER BY on %s appears faster (planner likely used the secondary index)\n", q.name)
        } else if avgWith > avgNo {
            fmt.Printf("  ⚠ ORDER BY on %s is slower or equal (planner may already use primary/other index)\n", q.name)
        } else {
            fmt.Printf("  ℹ No measurable difference for %s\n", q.name)
        }
    }

    fmt.Println("\n=== Comparison complete ===")
    return nil
}