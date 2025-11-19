package main

import (
	"context"
	"fmt"
	"log"

	"DBTests/Config"
	immusql "DBTests/IMMUSQL"
)

// simulateAllOperations demonstrates all available functions in Operations.go
func simulateAllOperations() {
	ctx := context.Background()

	// Initialize TableOps
	tableOps := immusql.GetTableOps()
	fmt.Println("=== ImmutableDB Operations Simulation ===")
	fmt.Println()

	// 1. Create Table
	fmt.Println("1. Creating table...")
	err := tableOps.CreateTable(ctx, Config.ImmuDBTable)
	if err != nil {
		log.Fatalf("Failed to create table: %v", err)
	}
	fmt.Printf("✓ Table '%s' created successfully\n\n", Config.ImmuDBTable)

	// 2. Insert Multiple Records
	fmt.Println("2. Inserting sample records...")
	sampleRecords := []Config.Transfer{
		{
			From:            "0x1111111111111111111111111111111111111111",
			To:              "0x2222222222222222222222222222222222222222",
			BlockNumber:     1001,
			TransactionHash: "0xaaaa", // Shorter hash to fit PRIMARY KEY length limit
		},
		{
			From:            "0x1111111111111111111111111111111111111111", // Same from address
			To:              "0x3333333333333333333333333333333333333333",
			BlockNumber:     1002,
			TransactionHash: "0xbbbb", // Shorter hash to fit PRIMARY KEY length limit
		},
		{
			From:            "0x4444444444444444444444444444444444444444",
			To:              "0x2222222222222222222222222222222222222222", // Same to address
			BlockNumber:     1002,                                         // Same block number
			TransactionHash: "0xcccc",                                     // Shorter hash to fit PRIMARY KEY length limit
		},
		{
			From:            "0x5555555555555555555555555555555555555555",
			To:              "0x6666666666666666666666666666666666666666",
			BlockNumber:     1003,
			TransactionHash: "0xdddd", // Shorter hash to fit PRIMARY KEY length limit
		},
	}

	err = tableOps.InsertRecords(ctx, sampleRecords)
	if err != nil {
		log.Fatalf("Failed to insert records: %v", err)
	}
	fmt.Printf("✓ Inserted %d records\n", len(sampleRecords))
	fmt.Println()

	// 3. Query Record by TransactionHash
	fmt.Println("3. Querying record by transaction hash...")
	queryHash := sampleRecords[0].TransactionHash
	record, err := tableOps.QueryRecord(ctx, queryHash)
	if err != nil {
		log.Fatalf("Failed to query record: %v", err)
	}
	if record != nil {
		fmt.Printf("✓ Found record:\n")
		fmt.Printf("  From: %s\n", record.From)
		fmt.Printf("  To: %s\n", record.To)
		fmt.Printf("  BlockNumber: %d\n", record.BlockNumber)
		fmt.Printf("  TransactionHash: %s\n", record.TransactionHash)
		fmt.Printf("  Timestamp: %d\n", record.Timestamp)
	} else {
		fmt.Println("✗ Record not found")
	}
	fmt.Println()

	// 4. Query Records by From Address
	fmt.Println("4. Querying records by FROM address...")
	fromAddress := "0x1111111111111111111111111111111111111111"
	recordsByFrom, err := tableOps.QueryRecordsByFrom(ctx, fromAddress)
	if err != nil {
		log.Fatalf("Failed to query records by from: %v", err)
	}
	fmt.Printf("✓ Found %d record(s) from address %s:\n", len(recordsByFrom), fromAddress)
	for i, rec := range recordsByFrom {
		fmt.Printf("  Record %d: %s -> %s (Block: %d)\n", i+1, rec.From, rec.To, rec.BlockNumber)
	}
	fmt.Println()

	// 5. Query Records by To Address
	fmt.Println("5. Querying records by TO address...")
	toAddress := "0x2222222222222222222222222222222222222222"
	recordsByTo, err := tableOps.QueryRecordsByTo(ctx, toAddress)
	if err != nil {
		log.Fatalf("Failed to query records by to: %v", err)
	}
	fmt.Printf("✓ Found %d record(s) to address %s:\n", len(recordsByTo), toAddress)
	for i, rec := range recordsByTo {
		fmt.Printf("  Record %d: %s -> %s (Block: %d)\n", i+1, rec.From, rec.To, rec.BlockNumber)
	}
	fmt.Println()

	// 6. Query Records by Block Number
	fmt.Println("6. Querying records by block number...")
	blockNumber := 1002
	recordsByBlock, err := tableOps.QueryRecordsByBlockNumber(ctx, blockNumber)
	if err != nil {
		log.Fatalf("Failed to query records by block number: %v", err)
	}
	fmt.Printf("✓ Found %d record(s) in block %d:\n", len(recordsByBlock), blockNumber)
	for i, rec := range recordsByBlock {
		hashDisplay := rec.TransactionHash

		fmt.Printf("  Record %d: %s -> %s (Hash: %s)\n", i+1, rec.From, rec.To, hashDisplay)
	}
	fmt.Println()

	// 7. Count Records by From Address
	fmt.Println("7. Counting records by FROM address...")
	countFrom, err := tableOps.CountRecords(ctx, fromAddress)
	if err != nil {
		log.Fatalf("Failed to count records by from: %v", err)
	}
	fmt.Printf("✓ Total records from %s: %d\n", fromAddress, countFrom)
	fmt.Println()

	// 8. Count Records by To Address
	fmt.Println("8. Counting records by TO address...")
	countTo, err := tableOps.CountRecordsTo(ctx, toAddress)
	if err != nil {
		log.Fatalf("Failed to count records by to: %v", err)
	}
	fmt.Printf("✓ Total records to %s: %d\n", toAddress, countTo)
	fmt.Println()

	// 9. Query Non-Existent Record
	fmt.Println("9. Querying non-existent record...")
	nonExistentHash := "0xffff" // Shorter hash to match format
	nonExistentRecord, err := tableOps.QueryRecord(ctx, nonExistentHash)
	if err != nil {
		log.Fatalf("Unexpected error querying non-existent record: %v", err)
	}
	if nonExistentRecord == nil {
		fmt.Println("✓ Correctly returned nil for non-existent record")
	} else {
		fmt.Println("✗ Unexpected record found")
	}
	fmt.Println()

	fmt.Println("=== All Operations Completed Successfully ===")
}

func main() {
	simulateAllOperations()
}
