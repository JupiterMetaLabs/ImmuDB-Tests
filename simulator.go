package main

import (
	"bufio"
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"fmt"
	"log"
	"math/big"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"DBTests/Config"
	immusql "DBTests/IMMUSQL"
)

// TestConfig holds all configurable test parameters
type TestConfig struct {
	TransactionCount    int  // Total number of transactions to generate and insert
	BatchSize           int  // Batch size for inserts (0 = use default from InsertRecords)
	QueryHashCount      int  // Number of hash queries to run for statistics
	QueryFromCount      int  // Number of FROM address queries to run
	QueryToCount        int  // Number of TO address queries to run
	QueryBlockCount     int  // Number of block number queries to run
	BlockNumberMin      int  // Minimum block number for test data
	BlockNumberMax      int  // Maximum block number for test data
	WarmupQueries       int  // Number of warmup queries before timing
	EnablePercentiles   bool // Calculate latency percentiles
	EnableDetailedStats bool // Enable detailed statistics collection
}

// IndexPerformanceConfig holds configuration for index performance testing
type IndexPerformanceConfig struct {
	TotalTransactions   int     // Total transactions to insert
	TxnsPerBlock        int     // Transactions per block (max 200)
	StartBlockNumber    int     // Starting block number
	RandomReadCount     int     // Number of random read queries to perform
	ReadHashRatio       float64 // Ratio of hash queries (0.0-1.0)
	ReadFromRatio       float64 // Ratio of FROM address queries (0.0-1.0)
	ReadToRatio         float64 // Ratio of TO address queries (0.0-1.0)
	ReadBlockRatio      float64 // Ratio of block number queries (0.0-1.0)
	EnablePercentiles   bool    // Calculate latency percentiles
	EnableDetailedStats bool    // Enable detailed statistics collection
}

// DefaultTestConfig returns a default test configuration
func DefaultTestConfig() TestConfig {
	return TestConfig{
		TransactionCount:    100000,
		BatchSize:           0, // Use default
		QueryHashCount:      500,
		QueryFromCount:      500,
		QueryToCount:        500,
		QueryBlockCount:     100,
		BlockNumberMin:      1000000,
		BlockNumberMax:      2000000,
		WarmupQueries:       5,
		EnablePercentiles:   true,
		EnableDetailedStats: true,
	}
}

// DefaultIndexPerformanceConfig returns default index performance test configuration
func DefaultIndexPerformanceConfig() IndexPerformanceConfig {
	return IndexPerformanceConfig{
		TotalTransactions:   200000,  // 50k transactions
		TxnsPerBlock:        200,     // Up to 200 txns per block (realistic)
		StartBlockNumber:    1000000, // Start from block 1M
		RandomReadCount:     1000,    // 1000 random reads
		ReadHashRatio:       0.40,    // 40% hash queries (explorer tx lookup)
		ReadFromRatio:       0.25,    // 25% FROM queries (address explorer)
		ReadToRatio:         0.25,    // 25% TO queries (address explorer)
		ReadBlockRatio:      0.10,    // 10% block queries (block explorer)
		EnablePercentiles:   true,
		EnableDetailedStats: true,
	}
}

// LatencyStats holds latency statistics
type LatencyStats struct {
	Count     int
	Min       time.Duration
	Max       time.Duration
	Mean      time.Duration
	P50       time.Duration // Median
	P95       time.Duration
	P99       time.Duration
	P999      time.Duration
	Total     time.Duration
	Durations []time.Duration // Only populated if EnableDetailedStats
}

// calculateLatencyStats calculates statistics from a slice of durations
func calculateLatencyStats(durations []time.Duration, enablePercentiles bool) LatencyStats {
	if len(durations) == 0 {
		return LatencyStats{}
	}

	stats := LatencyStats{
		Count: len(durations),
		Min:   durations[0],
		Max:   durations[0],
		Total: 0,
	}

	// Sort for percentile calculation
	sorted := make([]time.Duration, len(durations))
	copy(sorted, durations)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i] < sorted[j]
	})

	var total time.Duration
	for _, d := range sorted {
		total += d
		if d < stats.Min {
			stats.Min = d
		}
		if d > stats.Max {
			stats.Max = d
		}
	}

	stats.Total = total
	stats.Mean = total / time.Duration(len(sorted))

	if enablePercentiles && len(sorted) > 0 {
		stats.P50 = percentile(sorted, 0.50)
		stats.P95 = percentile(sorted, 0.95)
		stats.P99 = percentile(sorted, 0.99)
		stats.P999 = percentile(sorted, 0.999)
	} else {
		// Use median as P50
		if len(sorted) > 0 {
			stats.P50 = sorted[len(sorted)/2]
		}
	}

	if enablePercentiles {
		stats.Durations = sorted
	}

	return stats
}

// percentile calculates the percentile value from a sorted slice
func percentile(sorted []time.Duration, p float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	index := int(float64(len(sorted)) * p)
	if index >= len(sorted) {
		index = len(sorted) - 1
	}
	return sorted[index]
}

// Real Ethereum addresses for testing (42 characters each: 0x + 40 hex)
var testAddresses = []string{
	"0x742d35Cc6634C0532925a3b844Bc9e7595f0bEb0",
	"0x8ba1f109551bD432803012645Aac136c22C929E7",
	"0x1234567890123456789012345678901234567890",
	"0xabcdefabcdefabcdefabcdefabcdefabcdefabcd",
	"0xfedcba9876543210fedcba9876543210fedcba98",
}

// generateTransactionHash generates a realistic 66-character transaction hash
func generateTransactionHash() string {
	bytes := make([]byte, 32) // 32 bytes = 64 hex chars
	rand.Read(bytes)
	return "0x" + hex.EncodeToString(bytes)
}

// generateRandomBlockNumber generates a random block number between start and end
func generateRandomBlockNumber(start, end int) int {
	n, _ := rand.Int(rand.Reader, big.NewInt(int64(end-start+1)))
	return start + int(n.Int64())
}

// generateTestTransactions generates a specified number of test transactions
func generateTestTransactions(count int, blockMin, blockMax int) []Config.Transfer {
	transactions := make([]Config.Transfer, 0, count)
	baseTime := time.Now().Unix()

	for i := 0; i < count; i++ {
		// Randomly select from and to addresses from test addresses
		fromIdx := i % len(testAddresses)
		toIdx := (i + 1) % len(testAddresses)

		// Generate random block number
		blockNumber := generateRandomBlockNumber(blockMin, blockMax)

		// Generate realistic transaction hash
		txHash := generateTransactionHash()

		// Generate block hash (same format as transaction hash)
		blockHash := generateTransactionHash()

		transactions = append(transactions, Config.Transfer{
			From:            testAddresses[fromIdx],
			To:              testAddresses[toIdx],
			BlockNumber:     blockNumber,
			TransactionHash: txHash,
			BlockHash:       blockHash,
			TxBlockIndex:    i % 100,             // Transaction index within block (0-99)
			Timestamp:       baseTime + int64(i), // Incrementing timestamps
		})
	}

	return transactions
}

// generateBlockBasedTransactions generates transactions grouped by blocks (realistic pattern)
// Each block has up to txnsPerBlock transactions
func generateBlockBasedTransactions(totalTxns int, txnsPerBlock int, startBlock int) []Config.Transfer {
	transactions := make([]Config.Transfer, 0, totalTxns)
	baseTime := time.Now().Unix()

	currentBlock := startBlock
	txnsInCurrentBlock := 0
	blockHash := generateTransactionHash() // Same hash for all txns in a block

	for i := 0; i < totalTxns; i++ {
		// Start new block if current block is full
		if txnsInCurrentBlock >= txnsPerBlock {
			currentBlock++
			txnsInCurrentBlock = 0
			blockHash = generateTransactionHash() // New block hash
		}

		// Randomly select from and to addresses
		fromIdx := i % len(testAddresses)
		toIdx := (i + 1) % len(testAddresses)

		// Generate unique transaction hash
		txHash := generateTransactionHash()

		transactions = append(transactions, Config.Transfer{
			From:            testAddresses[fromIdx],
			To:              testAddresses[toIdx],
			BlockNumber:     currentBlock,
			TransactionHash: txHash,
			BlockHash:       blockHash,
			TxBlockIndex:    txnsInCurrentBlock,
			Timestamp:       baseTime + int64(i),
		})

		txnsInCurrentBlock++
	}

	return transactions
}

// printLatencyStats prints formatted latency statistics
func printLatencyStats(name string, stats LatencyStats) {
	fmt.Printf("  %s:\n", name)
	fmt.Printf("    Count:     %d\n", stats.Count)
	fmt.Printf("    Min:       %v\n", stats.Min)
	fmt.Printf("    Max:       %v\n", stats.Max)
	fmt.Printf("    Mean:      %v\n", stats.Mean)
	if stats.P50 > 0 {
		fmt.Printf("    P50:       %v\n", stats.P50)
	}
	if stats.P95 > 0 {
		fmt.Printf("    P95:       %v\n", stats.P95)
	}
	if stats.P99 > 0 {
		fmt.Printf("    P99:       %v\n", stats.P99)
	}
	if stats.P999 > 0 {
		fmt.Printf("    P99.9:     %v\n", stats.P999)
	}
	if stats.Count > 0 {
		throughput := float64(stats.Count) / stats.Total.Seconds()
		fmt.Printf("    Throughput: %.2f ops/s\n", throughput)
	}
}

// runPerformanceTest runs comprehensive performance tests with configurable parameters
func runPerformanceTest(config TestConfig) {
	ctx := context.Background()
	overallStart := time.Now()

	// Initialize TableOps
	tableOps := immusql.GetTableOps()
	fmt.Println("=== ImmutableDB Performance Test Simulator ===")
	fmt.Println()
	fmt.Println("Test Configuration:")
	fmt.Printf("  Transaction Count: %d\n", config.TransactionCount)
	fmt.Printf("  Query Hash Count:  %d\n", config.QueryHashCount)
	fmt.Printf("  Query From Count:  %d\n", config.QueryFromCount)
	fmt.Printf("  Query To Count:    %d\n", config.QueryToCount)
	fmt.Printf("  Query Block Count: %d\n", config.QueryBlockCount)
	fmt.Printf("  Block Range:       %d - %d\n", config.BlockNumberMin, config.BlockNumberMax)
	fmt.Println()

	// 1. Create Table
	fmt.Println("1. Creating table...")
	tableStart := time.Now()

	// Check if table has data - if so, indexes can't be created
	totalCount, countErr := tableOps.CountAllRecords(ctx)
	if countErr == nil && totalCount > 0 {
		fmt.Printf("⚠ Table already has %d records.\n", totalCount)
		fmt.Println("   ImmutableDB requires indexes to be created on empty tables.")
		fmt.Println("   To enable indexes, you need to:")
		fmt.Println("   1. Drop the table (loses all data)")
		fmt.Println("   2. Recreate table (indexes will be created on empty table)")
		fmt.Println("   3. Then insert data")
		fmt.Println()
		fmt.Println("   Current queries will use full table scans (1-3s per query).")
		fmt.Println("   Continuing with existing table (no indexes)...")
		fmt.Println()
	}

	err := tableOps.CreateTable(ctx, Config.ImmuDBTable)
	if err != nil {
		log.Fatalf("Failed to create table: %v", err)
	}
	tableCreateDuration := time.Since(tableStart)
	fmt.Printf("✓ Table '%s' created successfully in %v\n\n", Config.ImmuDBTable, tableCreateDuration)

	// 1.1. Test index performance if table has data
	if countErr == nil && totalCount > 0 {
		fmt.Println("1.1. Testing index performance on existing data...")
		testErr := tableOps.TestIndexPerformance(ctx, Config.ImmuDBTable)
		if testErr != nil {
			fmt.Printf("  Note: Index test failed: %v\n", testErr)
		}
		fmt.Println()
	}

	// 2. Generate test transactions
	fmt.Printf("2. Generating %d test transactions...\n", config.TransactionCount)
	generateStart := time.Now()
	transactions := generateTestTransactions(config.TransactionCount, config.BlockNumberMin, config.BlockNumberMax)
	generateDuration := time.Since(generateStart)
	generateRate := float64(config.TransactionCount) / generateDuration.Seconds()
	fmt.Printf("✓ Generated %d transactions in %v (%.2f tx/s)\n", len(transactions), generateDuration, generateRate)
	fmt.Printf("  Using %d test addresses\n\n", len(testAddresses))

	// 3. Batch insert all transactions
	fmt.Printf("3. Inserting %d transactions...\n", config.TransactionCount)
	insertStart := time.Now()
	err = tableOps.InsertRecords(ctx, transactions)
	if err != nil {
		log.Fatalf("Failed to insert records: %v", err)
	}
	insertDuration := time.Since(insertStart)
	insertRate := float64(config.TransactionCount) / insertDuration.Seconds()
	avgInsertTime := insertDuration / time.Duration(config.TransactionCount)
	fmt.Printf("✓ Inserted %d records in %v\n", config.TransactionCount, insertDuration)
	fmt.Printf("  Insert rate: %.2f records/second\n", insertRate)
	fmt.Printf("  Average time per record: %v\n\n", avgInsertTime)

	// 3.2: Get tail record
	fmt.Println("3.2. Getting tail record (highest ID)...")
	tailStart := time.Now()
	tailRecord, tailID, err := tableOps.GetTailRecord(ctx)
	if err != nil {
		log.Fatalf("Failed to get tail record: %v", err)
	}
	tailDuration := time.Since(tailStart)
	if tailRecord != nil {
		fmt.Printf("✓ Tail record ID: %d (queried in %v)\n", tailID, tailDuration)
		fmt.Printf("  Tail record: %s -> %s (Block: %d)\n",
			tailRecord.From, tailRecord.To, tailRecord.BlockNumber)
	} else {
		fmt.Printf("✓ No records found in table\n")
	}
	fmt.Println()

	// 4. Test queries by transaction hash with detailed stats
	fmt.Printf("4. Testing query by transaction hash (%d queries)...\n", config.QueryHashCount)

	// Check if queries are slow (likely due to missing indexes)
	fmt.Println("  ⚠ Note: If queries take >100ms, indexes may not be working properly")
	fmt.Println("  ⚠ Expected indexed query time: <50ms for 200k+ records")
	fmt.Println()

	hashDurations := make([]time.Duration, 0, config.QueryHashCount)

	// Warmup queries
	if config.WarmupQueries > 0 {
		fmt.Printf("  Running %d warmup queries...\n", config.WarmupQueries)
		for i := 0; i < config.WarmupQueries; i++ {
			testHash := transactions[i%len(transactions)].TransactionHash
			_, _ = tableOps.QueryRecord(ctx, testHash)
		}
	}

	// Timed queries with progress indicator
	fmt.Printf("  Running %d timed queries", config.QueryHashCount)
	if config.QueryHashCount > 20 {
		fmt.Printf(" (showing progress every 10%%)")
	}
	fmt.Println()

	progressInterval := config.QueryHashCount / 10
	if progressInterval == 0 {
		progressInterval = 1
	}

	for i := 0; i < config.QueryHashCount; i++ {
		testHash := transactions[i%len(transactions)].TransactionHash
		queryStart := time.Now()
		record, err := tableOps.QueryRecord(ctx, testHash)
		duration := time.Since(queryStart)
		hashDurations = append(hashDurations, duration)

		if err != nil && err != sql.ErrNoRows {
			log.Fatalf("Failed to query record: %v", err)
		}
		if i == 0 && record != nil {
			fmt.Printf("  Sample result: %s -> %s (Block: %d)\n",
				record.From, record.To, record.BlockNumber)
		}

		// Show progress for large query counts
		if config.QueryHashCount > 20 && (i+1)%progressInterval == 0 {
			percent := float64(i+1) / float64(config.QueryHashCount) * 100
			fmt.Printf("  Progress: %d/%d (%.0f%%) - Last query: %v\n",
				i+1, config.QueryHashCount, percent, duration)
		}
	}
	hashStats := calculateLatencyStats(hashDurations, config.EnablePercentiles)
	fmt.Printf("✓ Completed %d hash queries\n", config.QueryHashCount)

	// Performance warning
	if hashStats.Mean > 100*time.Millisecond {
		fmt.Printf("  ⚠ WARNING: Hash queries are slow (avg: %v)\n", hashStats.Mean)
		fmt.Printf("  ⚠ This suggests indexes may not be working. Expected: <50ms\n")
		fmt.Printf("  ⚠ Check index creation logs above for errors\n")
	}

	if config.EnableDetailedStats {
		printLatencyStats("Hash Query Latency", hashStats)
	} else {
		fmt.Printf("  Average: %v\n", hashStats.Mean)
	}
	fmt.Println()

	// 5. Test query by FROM address
	fmt.Printf("5. Testing query by FROM address (%d queries)...\n", config.QueryFromCount)
	fromDurations := make([]time.Duration, 0, config.QueryFromCount)
	var totalFromRecords int

	for i := 0; i < config.QueryFromCount; i++ {
		testFromAddress := testAddresses[i%len(testAddresses)]
		queryStart := time.Now()
		recordsByFrom, err := tableOps.QueryRecordsByFrom(ctx, testFromAddress)
		duration := time.Since(queryStart)
		fromDurations = append(fromDurations, duration)

		if err != nil {
			log.Fatalf("Failed to query records by from: %v", err)
		}
		totalFromRecords += len(recordsByFrom)
		if i == 0 && len(recordsByFrom) > 0 {
			fmt.Printf("  Sample: Found %d record(s) from %s (query took %v)\n",
				len(recordsByFrom), testFromAddress, duration)
		}
		if config.QueryFromCount > 5 && i > 0 {
			fmt.Printf("  Query %d/%d: %d records in %v\n",
				i+1, config.QueryFromCount, len(recordsByFrom), duration)
		}
	}
	fromStats := calculateLatencyStats(fromDurations, config.EnablePercentiles)
	avgFromRecords := float64(totalFromRecords) / float64(config.QueryFromCount)
	fmt.Printf("✓ Completed %d FROM queries (avg %.1f records per query)\n", config.QueryFromCount, avgFromRecords)
	if config.EnableDetailedStats {
		printLatencyStats("FROM Query Latency", fromStats)
	} else {
		fmt.Printf("  Average: %v\n", fromStats.Mean)
	}
	fmt.Println()

	// 6. Test query by TO address
	fmt.Printf("6. Testing query by TO address (%d queries)...\n", config.QueryToCount)
	toDurations := make([]time.Duration, 0, config.QueryToCount)
	var totalToRecords int

	for i := 0; i < config.QueryToCount; i++ {
		testToAddress := testAddresses[i%len(testAddresses)]
		queryStart := time.Now()
		recordsByTo, err := tableOps.QueryRecordsByTo(ctx, testToAddress)
		duration := time.Since(queryStart)
		toDurations = append(toDurations, duration)

		if err != nil {
			log.Fatalf("Failed to query records by to: %v", err)
		}
		totalToRecords += len(recordsByTo)
		if i == 0 && len(recordsByTo) > 0 {
			fmt.Printf("  Sample: Found %d record(s) to %s (query took %v)\n",
				len(recordsByTo), testToAddress, duration)
		}
		if config.QueryToCount > 5 && i > 0 {
			fmt.Printf("  Query %d/%d: %d records in %v\n",
				i+1, config.QueryToCount, len(recordsByTo), duration)
		}
	}
	toStats := calculateLatencyStats(toDurations, config.EnablePercentiles)
	avgToRecords := float64(totalToRecords) / float64(config.QueryToCount)
	fmt.Printf("✓ Completed %d TO queries (avg %.1f records per query)\n", config.QueryToCount, avgToRecords)
	if config.EnableDetailedStats {
		printLatencyStats("TO Query Latency", toStats)
	} else {
		fmt.Printf("  Average: %v\n", toStats.Mean)
	}
	fmt.Println()

	// 7. Test query by block number
	fmt.Printf("7. Testing query by block number (%d queries)...\n", config.QueryBlockCount)
	blockDurations := make([]time.Duration, 0, config.QueryBlockCount)
	var totalBlockRecords int

	for i := 0; i < config.QueryBlockCount; i++ {
		testBlockNumber := transactions[i%len(transactions)].BlockNumber
		queryStart := time.Now()
		recordsByBlock, err := tableOps.QueryRecordsByBlockNumber(ctx, testBlockNumber)
		duration := time.Since(queryStart)
		blockDurations = append(blockDurations, duration)

		if err != nil {
			log.Fatalf("Failed to query records by block number: %v", err)
		}
		totalBlockRecords += len(recordsByBlock)
		if i == 0 && len(recordsByBlock) > 0 {
			fmt.Printf("  Sample: Found %d record(s) in block %d (query took %v)\n",
				len(recordsByBlock), testBlockNumber, duration)
		}
		if config.QueryBlockCount > 5 && i > 0 {
			fmt.Printf("  Query %d/%d: %d records in %v\n",
				i+1, config.QueryBlockCount, len(recordsByBlock), duration)
		}
	}
	blockStats := calculateLatencyStats(blockDurations, config.EnablePercentiles)
	avgBlockRecords := float64(totalBlockRecords) / float64(config.QueryBlockCount)
	fmt.Printf("✓ Completed %d block queries (avg %.1f records per query)\n", config.QueryBlockCount, avgBlockRecords)
	if config.EnableDetailedStats {
		printLatencyStats("Block Query Latency", blockStats)
	} else {
		fmt.Printf("  Average: %v\n", blockStats.Mean)
	}
	fmt.Println()

	// 8. Test count by FROM address
	fmt.Println("8. Testing count by FROM address...")
	countFromStart := time.Now()
	testFromAddress := testAddresses[0]
	countFrom, err := tableOps.CountRecords(ctx, testFromAddress)
	if err != nil {
		log.Fatalf("Failed to count records by from: %v", err)
	}
	countFromDuration := time.Since(countFromStart)
	fmt.Printf("✓ Total records from %s: %d (queried in %v)\n", testFromAddress, countFrom, countFromDuration)
	fmt.Println()

	// 9. Test count by TO address
	fmt.Println("9. Testing count by TO address...")
	countToStart := time.Now()
	testToAddress := testAddresses[1]
	countTo, err := tableOps.CountRecordsTo(ctx, testToAddress)
	if err != nil {
		log.Fatalf("Failed to count records by to: %v", err)
	}
	countToDuration := time.Since(countToStart)
	fmt.Printf("✓ Total records to %s: %d (queried in %v)\n", testToAddress, countTo, countToDuration)
	fmt.Println()

	// 10. Get total record count
	fmt.Println("10. Getting total record count...")
	countAllStart := time.Now()
	totalCount, err = tableOps.CountAllRecords(ctx)
	if err != nil {
		log.Fatalf("Failed to count all records: %v", err)
	}
	countAllDuration := time.Since(countAllStart)
	fmt.Printf("✓ Total records in table: %d (queried in %v)\n", totalCount, countAllDuration)
	fmt.Println()

	// 11. Performance summary
	totalDuration := time.Since(overallStart)
	fmt.Println("=== Performance Summary ===")
	fmt.Println()
	fmt.Println("Operation Timings:")
	fmt.Printf("  Table Creation:     %v\n", tableCreateDuration)
	fmt.Printf("  Transaction Gen:     %v (%.2f tx/s)\n", generateDuration, generateRate)
	fmt.Printf("  Batch Insert:       %v (%.2f tx/s)\n", insertDuration, insertRate)
	fmt.Printf("  Tail Record Query:   %v\n", tailDuration)
	fmt.Printf("  Count All Records:   %v\n", countAllDuration)
	fmt.Println()

	if config.EnableDetailedStats {
		fmt.Println("Query Latency Statistics:")
		printLatencyStats("Hash Query", hashStats)
		printLatencyStats("FROM Query", fromStats)
		printLatencyStats("TO Query", toStats)
		printLatencyStats("Block Query", blockStats)
		fmt.Println()
	}

	fmt.Println("Count Operations:")
	fmt.Printf("  Count by From:       %v (count: %d)\n", countFromDuration, countFrom)
	fmt.Printf("  Count by To:         %v (count: %d)\n", countToDuration, countTo)
	fmt.Printf("  Count All:           %v (count: %d)\n", countAllDuration, totalCount)
	fmt.Println()

	fmt.Printf("Total Test Duration:   %v\n", totalDuration)
	fmt.Printf("Total Transactions:    %d\n", config.TransactionCount)
	fmt.Printf("Overall Throughput:    %.2f tx/s (including all operations)\n",
		float64(config.TransactionCount)/totalDuration.Seconds())
	fmt.Println()
	fmt.Println("✓ All performance tests completed successfully!")
}

// BenchmarkResult holds query performance results for comparison
type BenchmarkResult struct {
	HashStats    LatencyStats
	FromStats    LatencyStats
	ToStats      LatencyStats
	BlockStats   LatencyStats
	CountFrom    time.Duration
	CountTo      time.Duration
	CountAll     time.Duration
	InsertTime   time.Duration
	InsertRate   float64
	TotalRecords int
}

// runBenchmarkTest runs a performance test and returns results
func runBenchmarkTest(config TestConfig, withIndexes bool) BenchmarkResult {
	ctx := context.Background()
	tableOps := immusql.GetTableOps()

	// Drop table to ensure clean state
	fmt.Printf("Dropping existing table for clean benchmark...\n")
	tableOps.DropTable(ctx, Config.ImmuDBTable)

	// Create table with or without indexes
	if withIndexes {
		fmt.Println("Creating table WITH indexes...")
		err := tableOps.CreateTable(ctx, Config.ImmuDBTable)
		if err != nil {
			log.Fatalf("Failed to create table with indexes: %v", err)
		}
	} else {
		fmt.Println("Creating table WITHOUT indexes...")
		err := tableOps.CreateTableWithoutIndexes(ctx, Config.ImmuDBTable)
		if err != nil {
			log.Fatalf("Failed to create table without indexes: %v", err)
		}
	}

	// Generate transactions
	transactions := generateTestTransactions(config.TransactionCount, config.BlockNumberMin, config.BlockNumberMax)

	// Insert data
	insertStart := time.Now()
	err := tableOps.InsertRecords(ctx, transactions)
	if err != nil {
		log.Fatalf("Failed to insert records: %v", err)
	}
	insertDuration := time.Since(insertStart)
	insertRate := float64(config.TransactionCount) / insertDuration.Seconds()

	// Run queries
	hashDurations := make([]time.Duration, 0, config.QueryHashCount)
	for i := 0; i < config.QueryHashCount; i++ {
		testHash := transactions[i%len(transactions)].TransactionHash
		queryStart := time.Now()
		_, _ = tableOps.QueryRecord(ctx, testHash)
		hashDurations = append(hashDurations, time.Since(queryStart))
	}

	fromDurations := make([]time.Duration, 0, config.QueryFromCount)
	for i := 0; i < config.QueryFromCount; i++ {
		testFromAddress := testAddresses[i%len(testAddresses)]
		queryStart := time.Now()
		_, _ = tableOps.QueryRecordsByFrom(ctx, testFromAddress)
		fromDurations = append(fromDurations, time.Since(queryStart))
	}

	toDurations := make([]time.Duration, 0, config.QueryToCount)
	for i := 0; i < config.QueryToCount; i++ {
		testToAddress := testAddresses[i%len(testAddresses)]
		queryStart := time.Now()
		_, _ = tableOps.QueryRecordsByTo(ctx, testToAddress)
		toDurations = append(toDurations, time.Since(queryStart))
	}

	blockDurations := make([]time.Duration, 0, config.QueryBlockCount)
	for i := 0; i < config.QueryBlockCount; i++ {
		testBlockNumber := transactions[i%len(transactions)].BlockNumber
		queryStart := time.Now()
		_, _ = tableOps.QueryRecordsByBlockNumber(ctx, testBlockNumber)
		blockDurations = append(blockDurations, time.Since(queryStart))
	}

	// Count queries
	countFromStart := time.Now()
	testFromAddress := testAddresses[0]
	_, _ = tableOps.CountRecords(ctx, testFromAddress)
	countFromDuration := time.Since(countFromStart)

	countToStart := time.Now()
	testToAddress := testAddresses[1]
	_, _ = tableOps.CountRecordsTo(ctx, testToAddress)
	countToDuration := time.Since(countToStart)

	countAllStart := time.Now()
	totalCount, _ := tableOps.CountAllRecords(ctx)
	countAllDuration := time.Since(countAllStart)

	return BenchmarkResult{
		HashStats:    calculateLatencyStats(hashDurations, config.EnablePercentiles),
		FromStats:    calculateLatencyStats(fromDurations, config.EnablePercentiles),
		ToStats:      calculateLatencyStats(toDurations, config.EnablePercentiles),
		BlockStats:   calculateLatencyStats(blockDurations, config.EnablePercentiles),
		CountFrom:    countFromDuration,
		CountTo:      countToDuration,
		CountAll:     countAllDuration,
		InsertTime:   insertDuration,
		InsertRate:   insertRate,
		TotalRecords: totalCount,
	}
}

// runIndexBenchmarkComparison runs benchmark comparison with and without indexes
func runIndexBenchmarkComparison() {
	// Use a smaller config for faster benchmarking
	config := TestConfig{
		TransactionCount:    500000, // Smaller dataset for faster comparison
		QueryHashCount:      1000,
		QueryFromCount:      1000,
		QueryToCount:        1000,
		QueryBlockCount:     100,
		BlockNumberMin:      1000000,
		BlockNumberMax:      2000000,
		WarmupQueries:       0, // Skip warmup for cleaner comparison
		EnablePercentiles:   true,
		EnableDetailedStats: true,
	}

	fmt.Println("=== Index Benchmark Comparison ===")
	fmt.Println()
	fmt.Println("This will run the same test twice:")
	fmt.Println("  1. WITH indexes (table created, indexes added, data inserted)")
	fmt.Println("  2. WITHOUT indexes (table created, NO indexes, data inserted)")
	fmt.Println()
	fmt.Println("Test Configuration:")
	fmt.Printf("  Transaction Count: %d\n", config.TransactionCount)
	fmt.Printf("  Query Hash Count:  %d\n", config.QueryHashCount)
	fmt.Printf("  Query From Count:  %d\n", config.QueryFromCount)
	fmt.Printf("  Query To Count:    %d\n", config.QueryToCount)
	fmt.Printf("  Query Block Count: %d\n", config.QueryBlockCount)
	fmt.Println()
	fmt.Println("⚠ WARNING: This will drop and recreate the table!")
	fmt.Println("Press Enter to continue or Ctrl+C to cancel...")
	readInput()

	// Test WITH indexes
	fmt.Println()
	fmt.Println("═══════════════════════════════════════════════════════════")
	fmt.Println("TEST 1: WITH INDEXES")
	fmt.Println("═══════════════════════════════════════════════════════════")
	fmt.Println()
	withIndexesResult := runBenchmarkTest(config, true)

	// Small delay between tests
	time.Sleep(2 * time.Second)

	// Test WITHOUT indexes
	fmt.Println()
	fmt.Println("═══════════════════════════════════════════════════════════")
	fmt.Println("TEST 2: WITHOUT INDEXES")
	fmt.Println("═══════════════════════════════════════════════════════════")
	fmt.Println()
	withoutIndexesResult := runBenchmarkTest(config, false)

	// Comparison
	fmt.Println()
	fmt.Println("═══════════════════════════════════════════════════════════")
	fmt.Println("BENCHMARK COMPARISON RESULTS")
	fmt.Println("═══════════════════════════════════════════════════════════")
	fmt.Println()

	fmt.Printf("Dataset: %d records\n", config.TransactionCount)
	fmt.Println()

	// Hash Query Comparison
	fmt.Println("Hash Query Performance (point lookup):")
	fmt.Printf("  WITH indexes:    Mean: %v, P50: %v, P95: %v\n",
		withIndexesResult.HashStats.Mean, withIndexesResult.HashStats.P50, withIndexesResult.HashStats.P95)
	fmt.Printf("  WITHOUT indexes: Mean: %v, P50: %v, P95: %v\n",
		withoutIndexesResult.HashStats.Mean, withoutIndexesResult.HashStats.P50, withoutIndexesResult.HashStats.P95)
	if withoutIndexesResult.HashStats.Mean > 0 {
		speedup := float64(withoutIndexesResult.HashStats.Mean) / float64(withIndexesResult.HashStats.Mean)
		fmt.Printf("  Speedup: %.2fx %s\n", speedup,
			func() string {
				if speedup > 1 {
					return "faster with indexes"
				}
				return "slower with indexes (unexpected!)"
			}())
	}
	fmt.Println()

	// FROM Query Comparison
	fmt.Println("FROM Address Query Performance:")
	fmt.Printf("  WITH indexes:    Mean: %v, P50: %v, P95: %v\n",
		withIndexesResult.FromStats.Mean, withIndexesResult.FromStats.P50, withIndexesResult.FromStats.P95)
	fmt.Printf("  WITHOUT indexes: Mean: %v, P50: %v, P95: %v\n",
		withoutIndexesResult.FromStats.Mean, withoutIndexesResult.FromStats.P50, withoutIndexesResult.FromStats.P95)
	if withoutIndexesResult.FromStats.Mean > 0 {
		speedup := float64(withoutIndexesResult.FromStats.Mean) / float64(withIndexesResult.FromStats.Mean)
		fmt.Printf("  Speedup: %.2fx %s\n", speedup,
			func() string {
				if speedup > 1 {
					return "faster with indexes"
				}
				return "slower with indexes (unexpected!)"
			}())
	}
	fmt.Println()

	// TO Query Comparison
	fmt.Println("TO Address Query Performance:")
	fmt.Printf("  WITH indexes:    Mean: %v, P50: %v, P95: %v\n",
		withIndexesResult.ToStats.Mean, withIndexesResult.ToStats.P50, withIndexesResult.ToStats.P95)
	fmt.Printf("  WITHOUT indexes: Mean: %v, P50: %v, P95: %v\n",
		withoutIndexesResult.ToStats.Mean, withoutIndexesResult.ToStats.P50, withoutIndexesResult.ToStats.P95)
	if withoutIndexesResult.ToStats.Mean > 0 {
		speedup := float64(withoutIndexesResult.ToStats.Mean) / float64(withIndexesResult.ToStats.Mean)
		fmt.Printf("  Speedup: %.2fx %s\n", speedup,
			func() string {
				if speedup > 1 {
					return "faster with indexes"
				}
				return "slower with indexes (unexpected!)"
			}())
	}
	fmt.Println()

	// Block Query Comparison
	fmt.Println("Block Number Query Performance:")
	fmt.Printf("  WITH indexes:    Mean: %v, P50: %v, P95: %v\n",
		withIndexesResult.BlockStats.Mean, withIndexesResult.BlockStats.P50, withIndexesResult.BlockStats.P95)
	fmt.Printf("  WITHOUT indexes: Mean: %v, P50: %v, P95: %v\n",
		withoutIndexesResult.BlockStats.Mean, withoutIndexesResult.BlockStats.P50, withoutIndexesResult.BlockStats.P95)
	if withoutIndexesResult.BlockStats.Mean > 0 {
		speedup := float64(withoutIndexesResult.BlockStats.Mean) / float64(withIndexesResult.BlockStats.Mean)
		fmt.Printf("  Speedup: %.2fx %s\n", speedup,
			func() string {
				if speedup > 1 {
					return "faster with indexes"
				}
				return "slower with indexes (unexpected!)"
			}())
	}
	fmt.Println()

	// Count Query Comparison
	fmt.Println("Count Query Performance:")
	fmt.Printf("  Count FROM - WITH indexes:    %v\n", withIndexesResult.CountFrom)
	fmt.Printf("  Count FROM - WITHOUT indexes: %v\n", withoutIndexesResult.CountFrom)
	if withoutIndexesResult.CountFrom > 0 {
		speedup := float64(withoutIndexesResult.CountFrom) / float64(withIndexesResult.CountFrom)
		fmt.Printf("  Speedup: %.2fx %s\n", speedup,
			func() string {
				if speedup > 1 {
					return "faster with indexes"
				}
				return "slower with indexes (unexpected!)"
			}())
	}
	fmt.Println()

	// Insert Performance
	fmt.Println("Insert Performance:")
	fmt.Printf("  WITH indexes:    %v (%.2f tx/s)\n", withIndexesResult.InsertTime, withIndexesResult.InsertRate)
	fmt.Printf("  WITHOUT indexes: %v (%.2f tx/s)\n", withoutIndexesResult.InsertTime, withoutIndexesResult.InsertRate)
	fmt.Println()

	// Summary
	fmt.Println("═══════════════════════════════════════════════════════════")
	fmt.Println("SUMMARY")
	fmt.Println("═══════════════════════════════════════════════════════════")
	fmt.Println()

	avgSpeedup := 0.0
	count := 0

	if withoutIndexesResult.HashStats.Mean > 0 && withIndexesResult.HashStats.Mean > 0 {
		avgSpeedup += float64(withoutIndexesResult.HashStats.Mean) / float64(withIndexesResult.HashStats.Mean)
		count++
	}
	if withoutIndexesResult.FromStats.Mean > 0 && withIndexesResult.FromStats.Mean > 0 {
		avgSpeedup += float64(withoutIndexesResult.FromStats.Mean) / float64(withIndexesResult.FromStats.Mean)
		count++
	}
	if withoutIndexesResult.ToStats.Mean > 0 && withIndexesResult.ToStats.Mean > 0 {
		avgSpeedup += float64(withoutIndexesResult.ToStats.Mean) / float64(withIndexesResult.ToStats.Mean)
		count++
	}
	if withoutIndexesResult.BlockStats.Mean > 0 && withIndexesResult.BlockStats.Mean > 0 {
		avgSpeedup += float64(withoutIndexesResult.BlockStats.Mean) / float64(withIndexesResult.BlockStats.Mean)
		count++
	}

	if count > 0 {
		avgSpeedup /= float64(count)
		fmt.Printf("Average Query Speedup with Indexes: %.2fx\n", avgSpeedup)
		fmt.Println()
		if avgSpeedup > 1.5 {
			fmt.Println("✓ Indexes provide significant performance improvement")
		} else if avgSpeedup > 1.1 {
			fmt.Println("⚠ Indexes provide modest performance improvement")
		} else {
			fmt.Println("⚠ Indexes provide minimal or no performance improvement")
			fmt.Println("  This may indicate ImmutableDB query planner limitations")
		}
	}
	fmt.Println()
}

// queryTableState queries and displays the current state of the table
func queryTableState() {
	ctx := context.Background()
	tableOps := immusql.GetTableOps()

	fmt.Println("=== Querying Current Table State ===")
	fmt.Println()

	// Get total count
	totalCount, err := tableOps.CountAllRecords(ctx)
	if err != nil {
		log.Fatalf("Failed to get total count: %v", err)
	}

	if totalCount == 0 {
		fmt.Println("Table is empty (0 records)")
		return
	}

	fmt.Printf("Total Records: %d\n", totalCount)
	fmt.Println()

	// Get table statistics
	stats, err := tableOps.GetTableStatistics(ctx)
	if err != nil {
		log.Fatalf("Failed to get table statistics: %v", err)
	}

	fmt.Println("Table Statistics:")
	fmt.Printf("  Total Records:      %d\n", stats.TotalRecords)
	fmt.Printf("  Block Number Range: %d - %d\n", stats.MinBlockNumber, stats.MaxBlockNumber)
	if stats.UniqueFromAddrs >= 0 {
		fmt.Printf("  Unique From Addrs:  %d\n", stats.UniqueFromAddrs)
	} else {
		fmt.Printf("  Unique From Addrs:  N/A (query not supported)\n")
	}
	if stats.UniqueToAddrs >= 0 {
		fmt.Printf("  Unique To Addrs:    %d\n", stats.UniqueToAddrs)
	} else {
		fmt.Printf("  Unique To Addrs:    N/A (query not supported)\n")
	}
	if stats.MinTimestamp > 0 && stats.MaxTimestamp > 0 {
		minTime := time.Unix(stats.MinTimestamp, 0)
		maxTime := time.Unix(stats.MaxTimestamp, 0)
		fmt.Printf("  Timestamp Range:   %s - %s\n",
			minTime.Format("2006-01-02 15:04:05"),
			maxTime.Format("2006-01-02 15:04:05"))
	}
	fmt.Println()

	// Get head and tail records
	headRecord, headID, err := tableOps.GetHeadRecord(ctx)
	if err != nil {
		log.Fatalf("Failed to get head record: %v", err)
	}

	tailRecord, tailID, err := tableOps.GetTailRecord(ctx)
	if err != nil {
		log.Fatalf("Failed to get tail record: %v", err)
	}

	fmt.Println("Record Range:")
	if headRecord != nil {
		fmt.Printf("  First Record (ID %d):\n", headID)
		fmt.Printf("    Hash: %s\n", headRecord.TransactionHash)
		fmt.Printf("    From: %s -> To: %s\n", headRecord.From, headRecord.To)
		fmt.Printf("    Block: %d, Index: %d\n", headRecord.BlockNumber, headRecord.TxBlockIndex)
		fmt.Printf("    Time: %s\n", time.Unix(headRecord.Timestamp, 0).Format("2006-01-02 15:04:05"))
	}

	if tailRecord != nil {
		fmt.Printf("  Last Record (ID %d):\n", tailID)
		fmt.Printf("    Hash: %s\n", tailRecord.TransactionHash)
		fmt.Printf("    From: %s -> To: %s\n", tailRecord.From, tailRecord.To)
		fmt.Printf("    Block: %d, Index: %d\n", tailRecord.BlockNumber, tailRecord.TxBlockIndex)
		fmt.Printf("    Time: %s\n", time.Unix(tailRecord.Timestamp, 0).Format("2006-01-02 15:04:05"))
	}
	fmt.Println()

	// Get sample records
	sampleSize := 5
	if totalCount < sampleSize {
		sampleSize = totalCount
	}
	sampleRecords, err := tableOps.GetSampleRecords(ctx, sampleSize)
	if err != nil {
		log.Fatalf("Failed to get sample records: %v", err)
	}

	if len(sampleRecords) > 0 {
		fmt.Printf("Sample Records (first %d):\n", len(sampleRecords))
		for i, record := range sampleRecords {
			fmt.Printf("  [%d] %s -> %s (Block: %d, Hash: %s)\n",
				i+1, record.From, record.To, record.BlockNumber,
				record.TransactionHash[:20]+"...")
		}
		fmt.Println()
	}

	// Count by test addresses
	fmt.Println("Record Counts by Test Addresses:")
	for _, addr := range testAddresses {
		fromCount, _ := tableOps.CountRecords(ctx, addr)
		toCount, _ := tableOps.CountRecordsTo(ctx, addr)
		if fromCount > 0 || toCount > 0 {
			fmt.Printf("  %s:\n", addr)
			fmt.Printf("    From: %d records\n", fromCount)
			fmt.Printf("    To:   %d records\n", toCount)
		}
	}
	fmt.Println()

	fmt.Println("✓ Table state query completed")
}

// runIndexPerformanceTest runs index performance test with realistic workload
func runIndexPerformanceTest(config IndexPerformanceConfig) {
	ctx := context.Background()
	overallStart := time.Now()

	tableOps := immusql.GetTableOps()

	fmt.Println("=== Index Performance Test ===")
	fmt.Println()
	fmt.Println("Test Configuration:")
	fmt.Printf("  Total Transactions:  %d\n", config.TotalTransactions)
	fmt.Printf("  Transactions/Block:  %d (max)\n", config.TxnsPerBlock)
	fmt.Printf("  Start Block Number:  %d\n", config.StartBlockNumber)
	fmt.Printf("  Random Read Queries: %d\n", config.RandomReadCount)
	fmt.Printf("  Query Distribution:\n")
	fmt.Printf("    - Hash Queries:    %.1f%%\n", config.ReadHashRatio*100)
	fmt.Printf("    - FROM Queries:    %.1f%%\n", config.ReadFromRatio*100)
	fmt.Printf("    - TO Queries:      %.1f%%\n", config.ReadToRatio*100)
	fmt.Printf("    - Block Queries:   %.1f%%\n", config.ReadBlockRatio*100)
	fmt.Println()

	// 1. Create Table
	fmt.Println("1. Creating table with indexes...")
	tableStart := time.Now()
	err := tableOps.CreateTable(ctx, Config.ImmuDBTable)
	if err != nil {
		log.Fatalf("Failed to create table: %v", err)
	}
	tableCreateDuration := time.Since(tableStart)
	fmt.Printf("✓ Table created in %v\n\n", tableCreateDuration)

	// 2. Generate block-based transactions
	fmt.Printf("2. Generating %d transactions (block-based, up to %d per block)...\n",
		config.TotalTransactions, config.TxnsPerBlock)
	generateStart := time.Now()
	transactions := generateBlockBasedTransactions(
		config.TotalTransactions,
		config.TxnsPerBlock,
		config.StartBlockNumber,
	)
	generateDuration := time.Since(generateStart)
	numBlocks := transactions[len(transactions)-1].BlockNumber - transactions[0].BlockNumber + 1
	fmt.Printf("✓ Generated %d transactions in %d blocks in %v\n",
		len(transactions), numBlocks, generateDuration)
	fmt.Printf("  Avg txns per block: %.1f\n", float64(len(transactions))/float64(numBlocks))
	fmt.Println()

	// 3. Insert transactions (block-based, simulating real blockchain)
	fmt.Printf("3. Inserting %d transactions (block-by-block)...\n", config.TotalTransactions)
	insertStart := time.Now()

	// Group by block for realistic insertion
	blockGroups := make(map[int][]Config.Transfer)
	for _, tx := range transactions {
		blockGroups[tx.BlockNumber] = append(blockGroups[tx.BlockNumber], tx)
	}

	insertedCount := 0
	blockInsertDurations := make([]time.Duration, 0, len(blockGroups))

	for blockNum := config.StartBlockNumber; blockNum <= transactions[len(transactions)-1].BlockNumber; blockNum++ {
		if blockTxs, exists := blockGroups[blockNum]; exists {
			blockStart := time.Now()
			err = tableOps.InsertRecords(ctx, blockTxs)
			if err != nil {
				log.Fatalf("Failed to insert block %d: %v", blockNum, err)
			}
			blockDuration := time.Since(blockStart)
			blockInsertDurations = append(blockInsertDurations, blockDuration)
			insertedCount += len(blockTxs)

			if len(blockGroups) <= 20 || blockNum%10 == 0 {
				fmt.Printf("  Block %d: Inserted %d txns in %v\n",
					blockNum, len(blockTxs), blockDuration)
			}
		}
	}

	insertDuration := time.Since(insertStart)
	insertRate := float64(insertedCount) / insertDuration.Seconds()
	avgBlockInsertTime := insertDuration / time.Duration(len(blockInsertDurations))

	fmt.Printf("✓ Inserted %d transactions in %d blocks in %v\n",
		insertedCount, len(blockInsertDurations), insertDuration)
	fmt.Printf("  Insert rate: %.2f tx/s\n", insertRate)
	fmt.Printf("  Avg block insert time: %v\n", avgBlockInsertTime)
	fmt.Println()

	// 4. Random read queries (simulating explorer + business logic)
	fmt.Printf("4. Running %d random read queries (simulating explorer workload)...\n", config.RandomReadCount)

	// Calculate query counts based on ratios
	hashQueryCount := int(float64(config.RandomReadCount) * config.ReadHashRatio)
	fromQueryCount := int(float64(config.RandomReadCount) * config.ReadFromRatio)
	toQueryCount := int(float64(config.RandomReadCount) * config.ReadToRatio)
	blockQueryCount := config.RandomReadCount - hashQueryCount - fromQueryCount - toQueryCount

	fmt.Printf("  Query breakdown: %d hash, %d FROM, %d TO, %d block\n",
		hashQueryCount, fromQueryCount, toQueryCount, blockQueryCount)
	fmt.Println()

	// Hash queries (indexed on transactionHash)
	hashDurations := make([]time.Duration, 0, hashQueryCount)
	if hashQueryCount > 0 {
		fmt.Printf("  4.1. Hash Queries (%d) - Index: transactionHash\n", hashQueryCount)
		for i := 0; i < hashQueryCount; i++ {
			// Random transaction hash from inserted data
			randomIdx := i % len(transactions)
			testHash := transactions[randomIdx].TransactionHash

			queryStart := time.Now()
			_, err := tableOps.QueryRecord(ctx, testHash)
			duration := time.Since(queryStart)
			hashDurations = append(hashDurations, duration)

			if err != nil && err != sql.ErrNoRows {
				log.Fatalf("Failed to query by hash: %v", err)
			}

			if hashQueryCount > 50 && (i+1)%(hashQueryCount/10) == 0 {
				fmt.Printf("    Progress: %d/%d (%.0f%%)\n",
					i+1, hashQueryCount, float64(i+1)/float64(hashQueryCount)*100)
			}
		}
		hashStats := calculateLatencyStats(hashDurations, config.EnablePercentiles)
		fmt.Printf("  ✓ Hash queries completed\n")
		if config.EnableDetailedStats {
			printLatencyStats("    Hash Query (Indexed)", hashStats)
		}
		fmt.Println()
	}

	// FROM address queries (indexed on fromAddr)
	fromDurations := make([]time.Duration, 0, fromQueryCount)
	var totalFromRecords int
	if fromQueryCount > 0 {
		fmt.Printf("  4.2. FROM Address Queries (%d) - Index: fromAddr\n", fromQueryCount)
		for i := 0; i < fromQueryCount; i++ {
			// Random address from test addresses
			addrIdx := i % len(testAddresses)
			testFromAddress := testAddresses[addrIdx]

			queryStart := time.Now()
			records, err := tableOps.QueryRecordsByFrom(ctx, testFromAddress)
			duration := time.Since(queryStart)
			fromDurations = append(fromDurations, duration)

			if err != nil {
				log.Fatalf("Failed to query by FROM: %v", err)
			}
			totalFromRecords += len(records)

			if fromQueryCount > 20 && (i+1)%(fromQueryCount/5) == 0 {
				fmt.Printf("    Progress: %d/%d - Avg records: %.1f\n",
					i+1, fromQueryCount, float64(totalFromRecords)/float64(i+1))
			}
		}
		fromStats := calculateLatencyStats(fromDurations, config.EnablePercentiles)
		avgFromRecords := float64(totalFromRecords) / float64(fromQueryCount)
		fmt.Printf("  ✓ FROM queries completed (avg %.1f records per query)\n", avgFromRecords)
		if config.EnableDetailedStats {
			printLatencyStats("    FROM Query (Indexed)", fromStats)
		}
		fmt.Println()
	}

	// TO address queries (indexed on toAddr)
	toDurations := make([]time.Duration, 0, toQueryCount)
	var totalToRecords int
	if toQueryCount > 0 {
		fmt.Printf("  4.3. TO Address Queries (%d) - Index: toAddr\n", toQueryCount)
		for i := 0; i < toQueryCount; i++ {
			// Random address from test addresses
			addrIdx := i % len(testAddresses)
			testToAddress := testAddresses[addrIdx]

			queryStart := time.Now()
			records, err := tableOps.QueryRecordsByTo(ctx, testToAddress)
			duration := time.Since(queryStart)
			toDurations = append(toDurations, duration)

			if err != nil {
				log.Fatalf("Failed to query by TO: %v", err)
			}
			totalToRecords += len(records)

			if toQueryCount > 20 && (i+1)%(toQueryCount/5) == 0 {
				fmt.Printf("    Progress: %d/%d - Avg records: %.1f\n",
					i+1, toQueryCount, float64(totalToRecords)/float64(i+1))
			}
		}
		toStats := calculateLatencyStats(toDurations, config.EnablePercentiles)
		avgToRecords := float64(totalToRecords) / float64(toQueryCount)
		fmt.Printf("  ✓ TO queries completed (avg %.1f records per query)\n", avgToRecords)
		if config.EnableDetailedStats {
			printLatencyStats("    TO Query (Indexed)", toStats)
		}
		fmt.Println()
	}

	// Block number queries (no index - full table scan expected)
	blockDurations := make([]time.Duration, 0, blockQueryCount)
	var totalBlockRecords int
	if blockQueryCount > 0 {
		fmt.Printf("  4.4. Block Number Queries (%d) - No Index (Full Scan)\n", blockQueryCount)
		for i := 0; i < blockQueryCount; i++ {
			// Random block number from inserted data
			randomIdx := i % len(transactions)
			testBlockNumber := transactions[randomIdx].BlockNumber

			queryStart := time.Now()
			records, err := tableOps.QueryRecordsByBlockNumber(ctx, testBlockNumber)
			duration := time.Since(queryStart)
			blockDurations = append(blockDurations, duration)

			if err != nil {
				log.Fatalf("Failed to query by block: %v", err)
			}
			totalBlockRecords += len(records)

			if blockQueryCount > 20 && (i+1)%(blockQueryCount/5) == 0 {
				fmt.Printf("    Progress: %d/%d - Avg records: %.1f\n",
					i+1, blockQueryCount, float64(totalBlockRecords)/float64(i+1))
			}
		}
		blockStats := calculateLatencyStats(blockDurations, config.EnablePercentiles)
		avgBlockRecords := float64(totalBlockRecords) / float64(blockQueryCount)
		fmt.Printf("  ✓ Block queries completed (avg %.1f records per query)\n", avgBlockRecords)
		if config.EnableDetailedStats {
			printLatencyStats("    Block Query (No Index)", blockStats)
		}
		fmt.Println()
	}

	// 5. Index Performance Summary
	totalDuration := time.Since(overallStart)
	fmt.Println("=== Index Performance Summary ===")
	fmt.Println()

	fmt.Println("Insert Performance:")
	fmt.Printf("  Total Transactions: %d\n", insertedCount)
	fmt.Printf("  Total Blocks:       %d\n", len(blockInsertDurations))
	fmt.Printf("  Insert Duration:    %v\n", insertDuration)
	fmt.Printf("  Insert Throughput:  %.2f tx/s\n", insertRate)
	fmt.Printf("  Avg Block Insert:   %v\n", avgBlockInsertTime)
	fmt.Println()

	fmt.Println("Index Performance Comparison:")
	if hashQueryCount > 0 {
		hashStats := calculateLatencyStats(hashDurations, config.EnablePercentiles)
		fmt.Printf("  Hash Query (Indexed):     P50=%v, P95=%v, P99=%v\n",
			hashStats.P50, hashStats.P95, hashStats.P99)
	}
	if fromQueryCount > 0 {
		fromStats := calculateLatencyStats(fromDurations, config.EnablePercentiles)
		fmt.Printf("  FROM Query (Indexed):     P50=%v, P95=%v, P99=%v\n",
			fromStats.P50, fromStats.P95, fromStats.P99)
	}
	if toQueryCount > 0 {
		toStats := calculateLatencyStats(toDurations, config.EnablePercentiles)
		fmt.Printf("  TO Query (Indexed):       P50=%v, P95=%v, P99=%v\n",
			toStats.P50, toStats.P95, toStats.P99)
	}
	if blockQueryCount > 0 {
		blockStats := calculateLatencyStats(blockDurations, config.EnablePercentiles)
		fmt.Printf("  Block Query (No Index):   P50=%v, P95=%v, P99=%v\n",
			blockStats.P50, blockStats.P95, blockStats.P99)
	}
	fmt.Println()

	fmt.Printf("Total Test Duration: %v\n", totalDuration)
	fmt.Printf("Total Queries:      %d\n", config.RandomReadCount)
	fmt.Printf("Query Throughput:   %.2f queries/s\n",
		float64(config.RandomReadCount)/totalDuration.Seconds())
	fmt.Println()

	fmt.Println("✓ Index performance test completed!")
}

// printMenu displays the interactive menu
func printMenu() {
	fmt.Println()
	fmt.Println("=== ImmutableDB Test Simulator ===")
	fmt.Println()
	fmt.Println("Select an option:")
	fmt.Println("  1. Query Table State")
	fmt.Println("  2. Run Performance Test (default config)")
	fmt.Println("  3. Run Performance Test (custom config)")
	fmt.Println("  4. Run Index Performance Test (realistic workload)")
	fmt.Println("  5. Benchmark: With Indexes vs Without Indexes")
	fmt.Println("  6. Exit")
	fmt.Print("\nEnter choice (1-6): ")
}

// readInput reads a line from stdin
func readInput() string {
	reader := bufio.NewReader(os.Stdin)
	input, err := reader.ReadString('\n')
	if err != nil {
		log.Fatalf("Failed to read input: %v", err)
	}
	return strings.TrimSpace(input)
}

// configureTest allows interactive configuration of test parameters
func configureTest() TestConfig {
	config := DefaultTestConfig()
	reader := bufio.NewReader(os.Stdin)

	fmt.Println("\n=== Configure Performance Test ===")
	fmt.Println("Press Enter to use default values (shown in brackets)")
	fmt.Println()

	// Transaction count
	fmt.Printf("Transaction Count [%d]: ", config.TransactionCount)
	input, _ := reader.ReadString('\n')
	input = strings.TrimSpace(input)
	if input != "" {
		if val, err := strconv.Atoi(input); err == nil && val > 0 {
			config.TransactionCount = val
		}
	}

	// Query hash count
	fmt.Printf("Query Hash Count [%d]: ", config.QueryHashCount)
	input, _ = reader.ReadString('\n')
	input = strings.TrimSpace(input)
	if input != "" {
		if val, err := strconv.Atoi(input); err == nil && val > 0 {
			config.QueryHashCount = val
		}
	}

	// Query from count
	fmt.Printf("Query From Count [%d]: ", config.QueryFromCount)
	input, _ = reader.ReadString('\n')
	input = strings.TrimSpace(input)
	if input != "" {
		if val, err := strconv.Atoi(input); err == nil && val > 0 {
			config.QueryFromCount = val
		}
	}

	// Query to count
	fmt.Printf("Query To Count [%d]: ", config.QueryToCount)
	input, _ = reader.ReadString('\n')
	input = strings.TrimSpace(input)
	if input != "" {
		if val, err := strconv.Atoi(input); err == nil && val > 0 {
			config.QueryToCount = val
		}
	}

	// Query block count
	fmt.Printf("Query Block Count [%d]: ", config.QueryBlockCount)
	input, _ = reader.ReadString('\n')
	input = strings.TrimSpace(input)
	if input != "" {
		if val, err := strconv.Atoi(input); err == nil && val > 0 {
			config.QueryBlockCount = val
		}
	}

	// Block number range
	fmt.Printf("Block Number Min [%d]: ", config.BlockNumberMin)
	input, _ = reader.ReadString('\n')
	input = strings.TrimSpace(input)
	if input != "" {
		if val, err := strconv.Atoi(input); err == nil && val > 0 {
			config.BlockNumberMin = val
		}
	}

	fmt.Printf("Block Number Max [%d]: ", config.BlockNumberMax)
	input, _ = reader.ReadString('\n')
	input = strings.TrimSpace(input)
	if input != "" {
		if val, err := strconv.Atoi(input); err == nil && val > 0 {
			config.BlockNumberMax = val
		}
	}

	// Enable percentiles
	fmt.Printf("Enable Percentiles (y/n) [y]: ")
	input, _ = reader.ReadString('\n')
	input = strings.ToLower(strings.TrimSpace(input))
	if input == "n" || input == "no" {
		config.EnablePercentiles = false
	}

	// Enable detailed stats
	fmt.Printf("Enable Detailed Stats (y/n) [y]: ")
	input, _ = reader.ReadString('\n')
	input = strings.ToLower(strings.TrimSpace(input))
	if input == "n" || input == "no" {
		config.EnableDetailedStats = false
	}

	return config
}

// runInteractiveCLI runs the interactive command-line interface
func runInteractiveCLI() {
	for {
		printMenu()
		choice := readInput()

		switch choice {
		case "1":
			fmt.Println()
			queryTableState()
			fmt.Println("\nPress Enter to continue...")
			readInput()

		case "2":
			fmt.Println()
			config := DefaultTestConfig()
			runPerformanceTest(config)
			fmt.Println("\nPress Enter to continue...")
			readInput()

		case "3":
			config := configureTest()
			fmt.Println("\nStarting performance test with custom configuration...")
			fmt.Println()
			runPerformanceTest(config)
			fmt.Println("\nPress Enter to continue...")
			readInput()

		case "4":
			fmt.Println()
			indexConfig := DefaultIndexPerformanceConfig()
			runIndexPerformanceTest(indexConfig)
			fmt.Println("\nPress Enter to continue...")
			readInput()

		case "5":
			fmt.Println()
			runIndexBenchmarkComparison()
			fmt.Println("\nPress Enter to continue...")
			readInput()

		case "6", "q", "quit", "exit":
			fmt.Println("\nExiting...")
			return

		default:
			fmt.Printf("\nInvalid choice: %s. Please enter 1-6.\n", choice)
			time.Sleep(1 * time.Second)
		}
	}
}

func main() {
	// Check for command-line arguments for non-interactive mode
	if len(os.Args) > 1 {
		command := strings.ToLower(os.Args[1])
		switch command {
		case "query", "state", "status":
			queryTableState()
		case "test", "perf", "performance":
			config := DefaultTestConfig()
			runPerformanceTest(config)
		case "help", "-h", "--help":
			fmt.Println("Usage:")
			fmt.Println("  go run simulator.go              - Interactive mode")
			fmt.Println("  go run simulator.go query         - Query table state")
			fmt.Println("  go run simulator.go test          - Run performance test")
			fmt.Println("  go run simulator.go help          - Show this help")
		default:
			fmt.Printf("Unknown command: %s\n", command)
			fmt.Println("Use 'help' to see available commands")
			os.Exit(1)
		}
		return
	}

	// Run interactive CLI by default
	runInteractiveCLI()
}
