package main

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"fmt"
	"log"
	"math/big"
	"time"

	"DBTests/Config"
	immusql "DBTests/IMMUSQL"
)

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
func generateTestTransactions(count int) []Config.Transfer {
	transactions := make([]Config.Transfer, 0, count)
	baseTime := time.Now().Unix()

	for i := 0; i < count; i++ {
		// Randomly select from and to addresses from test addresses
		fromIdx := i % len(testAddresses)
		toIdx := (i + 1) % len(testAddresses)

		// Generate random block number between 1000000 and 2000000
		blockNumber := generateRandomBlockNumber(1000000, 2000000)

		// Generate realistic transaction hash
		txHash := generateTransactionHash()

		transactions = append(transactions, Config.Transfer{
			From:            testAddresses[fromIdx],
			To:              testAddresses[toIdx],
			BlockNumber:     blockNumber,
			TransactionHash: txHash,
			Timestamp:       baseTime + int64(i), // Incrementing timestamps
		})
	}

	return transactions
}

// runPerformanceTest runs comprehensive performance tests
func runPerformanceTest() {
	ctx := context.Background()
	overallStart := time.Now()

	// Initialize TableOps
	tableOps := immusql.GetTableOps()
	fmt.Println("=== ImmutableDB Performance Test Simulator ===")
	fmt.Println()

	// Performance metrics storage
	var (
		tableCreateDuration  time.Duration
		generateDuration     time.Duration
		insertDuration       time.Duration
		queryByHashDuration  time.Duration
		queryByFromDuration  time.Duration
		queryByToDuration    time.Duration
		queryByBlockDuration time.Duration
		countFromDuration    time.Duration
		countToDuration      time.Duration
	)

	// 1. Create Table
	fmt.Println("1. Creating table...")
	startTime := time.Now()
	err := tableOps.CreateTable(ctx, Config.ImmuDBTable)
	if err != nil {
		log.Fatalf("Failed to create table: %v", err)
	}
	tableCreateDuration = time.Since(startTime)
	fmt.Printf("✓ Table '%s' created successfully in %v\n\n", Config.ImmuDBTable, tableCreateDuration)

	// 2. Generate test transactions
	transactionCount := 100000
	fmt.Printf("2. Generating %d test transactions...\n", transactionCount)
	startTime = time.Now()
	transactions := generateTestTransactions(transactionCount)
	generateDuration = time.Since(startTime)
	fmt.Printf("✓ Generated %d transactions in %v\n", len(transactions), generateDuration)
	fmt.Printf("  Using %d test addresses: %v\n", len(testAddresses), testAddresses)
	fmt.Printf("  Generation rate: %.2f tx/s\n\n", float64(transactionCount)/generateDuration.Seconds())

	// 3. Batch insert all transactions
	fmt.Printf("3. Inserting %d transactions in batch...\n", transactionCount)
	startTime = time.Now()
	err = tableOps.InsertRecords(ctx, transactions)
	if err != nil {
		log.Fatalf("Failed to insert records: %v", err)
	}
	insertDuration = time.Since(startTime)
	fmt.Printf("✓ Inserted %d records in %v\n", transactionCount, insertDuration)
	fmt.Printf("  Insert rate: %.2f records/second\n", float64(transactionCount)/insertDuration.Seconds())
	fmt.Printf("  Average time per record: %v\n\n", insertDuration/time.Duration(transactionCount))

	// 3.2 : Get tail record (last inserted record with highest ID)
	fmt.Println("3.2. Getting tail record (highest ID)...")
	startTime = time.Now()
	tailRecord, tailID, err := tableOps.GetTailRecord(ctx)
	if err != nil {
		log.Fatalf("Failed to get tail record: %v", err)
	}
	tailDuration := time.Since(startTime)
	if tailRecord != nil {
		fmt.Printf("✓ Tail record ID: %d (queried in %v)\n", tailID, tailDuration)
		fmt.Printf("  Tail record: %s -> %s (Block: %d, Hash: %s)\n",
			tailRecord.From, tailRecord.To, tailRecord.BlockNumber, tailRecord.TransactionHash)
	} else {
		fmt.Printf("✓ No records found in table\n")
	}
	fmt.Println()

	// 4. Test queries by transaction hash (multiple queries for average)
	fmt.Println("4. Testing query by transaction hash...")
	queryCount := 10
	var totalQueryTime time.Duration
	for i := 0; i < queryCount; i++ {
		testHash := transactions[i%len(transactions)].TransactionHash
		startTime = time.Now()
		record, err := tableOps.QueryRecord(ctx, testHash)
		totalQueryTime += time.Since(startTime)
		if err != nil && err != sql.ErrNoRows {
			log.Fatalf("Failed to query record: %v", err)
		}
		if i == 0 && record != nil {
			fmt.Printf("  Sample result: %s -> %s (Block: %d)\n",
				record.From, record.To, record.BlockNumber)
		}
	}
	queryByHashDuration = totalQueryTime / time.Duration(queryCount)
	fmt.Printf("✓ Queried %d records (average: %v per query)\n", queryCount, queryByHashDuration)
	fmt.Println()

	// 5. Test query by FROM address
	fmt.Println("5. Testing query by FROM address...")
	testFromAddress := testAddresses[0]
	startTime = time.Now()
	recordsByFrom, err := tableOps.QueryRecordsByFrom(ctx, testFromAddress)
	if err != nil {
		log.Fatalf("Failed to query records by from: %v", err)
	}
	queryByFromDuration = time.Since(startTime)
	fmt.Printf("✓ Found %d record(s) from address %s in %v\n", len(recordsByFrom), testFromAddress, queryByFromDuration)
	if len(recordsByFrom) > 0 {
		fmt.Printf("  Sample record: %s -> %s (Block: %d)\n",
			recordsByFrom[0].From, recordsByFrom[0].To, recordsByFrom[0].BlockNumber)
	}
	fmt.Println()

	// 6. Test query by TO address
	fmt.Println("6. Testing query by TO address...")
	testToAddress := testAddresses[1]
	startTime = time.Now()
	recordsByTo, err := tableOps.QueryRecordsByTo(ctx, testToAddress)
	if err != nil {
		log.Fatalf("Failed to query records by to: %v", err)
	}
	queryByToDuration = time.Since(startTime)
	fmt.Printf("✓ Found %d record(s) to address %s in %v\n", len(recordsByTo), testToAddress, queryByToDuration)
	if len(recordsByTo) > 0 {
		fmt.Printf("  Sample record: %s -> %s (Block: %d)\n",
			recordsByTo[0].From, recordsByTo[0].To, recordsByTo[0].BlockNumber)
	}
	fmt.Println()

	// 7. Test query by block number
	fmt.Println("7. Testing query by block number...")
	testBlockNumber := transactions[0].BlockNumber
	startTime = time.Now()
	recordsByBlock, err := tableOps.QueryRecordsByBlockNumber(ctx, testBlockNumber)
	if err != nil {
		log.Fatalf("Failed to query records by block number: %v", err)
	}
	queryByBlockDuration = time.Since(startTime)
	fmt.Printf("✓ Found %d record(s) in block %d in %v\n", len(recordsByBlock), testBlockNumber, queryByBlockDuration)
	if len(recordsByBlock) > 0 {
		fmt.Printf("  Sample record: %s -> %s (Hash: %s)\n",
			recordsByBlock[0].From, recordsByBlock[0].To, recordsByBlock[0].TransactionHash)
	}
	fmt.Println()

	// 8. Test count by FROM address
	fmt.Println("8. Testing count by FROM address...")
	startTime = time.Now()
	countFrom, err := tableOps.CountRecords(ctx, testFromAddress)
	if err != nil {
		log.Fatalf("Failed to count records by from: %v", err)
	}
	countFromDuration = time.Since(startTime)
	fmt.Printf("✓ Total records from %s: %d (queried in %v)\n", testFromAddress, countFrom, countFromDuration)
	fmt.Println()

	// 9. Test count by TO address
	fmt.Println("9. Testing count by TO address...")
	startTime = time.Now()
	countTo, err := tableOps.CountRecordsTo(ctx, testToAddress)
	if err != nil {
		log.Fatalf("Failed to count records by to: %v", err)
	}
	countToDuration = time.Since(startTime)
	fmt.Printf("✓ Total records to %s: %d (queried in %v)\n", testToAddress, countTo, countToDuration)
	fmt.Println()

	// 10. Performance summary
	totalDuration := time.Since(overallStart)
	fmt.Println("=== Performance Summary ===")
	fmt.Println()
	fmt.Println("Operation Timings:")
	fmt.Printf("  Table Creation:     %v\n", tableCreateDuration)
	fmt.Printf("  Transaction Gen:     %v (%.2f tx/s)\n", generateDuration, float64(transactionCount)/generateDuration.Seconds())
	fmt.Printf("  Batch Insert:        %v (%.2f tx/s)\n", insertDuration, float64(transactionCount)/insertDuration.Seconds())
	fmt.Printf("  Query by Hash:       %v (avg of %d queries)\n", queryByHashDuration, queryCount)
	fmt.Printf("  Query by From:       %v (%d records)\n", queryByFromDuration, len(recordsByFrom))
	fmt.Printf("  Query by To:         %v (%d records)\n", queryByToDuration, len(recordsByTo))
	fmt.Printf("  Query by Block:      %v (%d records)\n", queryByBlockDuration, len(recordsByBlock))
	fmt.Printf("  Count by From:       %v (count: %d)\n", countFromDuration, countFrom)
	fmt.Printf("  Count by To:         %v (count: %d)\n", countToDuration, countTo)
	fmt.Println()
	fmt.Printf("Total Test Duration:   %v\n", totalDuration)
	fmt.Printf("Total Transactions:    %d\n", transactionCount)
	fmt.Println()
	fmt.Println("✓ All performance tests completed successfully!")
}

func main() {
	runPerformanceTest()
}
