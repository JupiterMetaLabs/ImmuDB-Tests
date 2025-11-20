package IMMUSQL

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"DBTests/Config"
	"DBTests/IMMUDB"
)

/*
- Operations using ImmutableDB SQL API
- Uses the native client connection from DB.go via stdlib wrapper
- All operations use standard SQL queries

IMPORTANT: Index Creation Requirements
======================================
ImmutableDB supports secondary indexes, but with a critical limitation:
- Indexes can ONLY be added to EMPTY tables
- If you try to create indexes on a table with data, they won't work

Correct workflow:
1. CREATE TABLE
2. CREATE INDEX ON table(column) - immediately, while table is empty
3. INSERT data - after indexes are created

If table already has data:
- Drop table (DROP TABLE tablename)
- Recreate table (this will create indexes on empty table)
- Then insert data

Syntax: CREATE INDEX ON table(column) - no explicit index names
Indexes are referenced by the ordered list of columns, not by name.

Performance expectations:
- With indexes: <50ms for point lookups
- Without indexes: 1-3s for full table scans
*/

type TableOps struct {
	DB *sql.DB
}

// GetTableOps creates and returns a TableOps instance with connected ImmutableDB database
func GetTableOps() *TableOps {
	db, err := IMMUDB.ConnectDB()
	if err != nil {
		panic(err)
	}
	return &TableOps{
		DB: db,
	}
}

// CreateTableWithoutIndexes creates a SQL table in ImmutableDB WITHOUT indexes
// This is used for benchmarking to compare performance with vs without indexes
func (t *TableOps) CreateTableWithoutIndexes(ctx context.Context, tableName string) error {
	createTableSQL := fmt.Sprintf(`
	CREATE TABLE %s (
		id INTEGER AUTO_INCREMENT,
		transactionHash VARCHAR[66] NOT NULL,
		fromAddr VARCHAR[42] NOT NULL,
		toAddr VARCHAR[42],
		blockNumber INTEGER NOT NULL,
		blockHash VARCHAR[66] NOT NULL,
		txBlockIndex INTEGER NOT NULL,
		ts TIMESTAMP NOT NULL,
		PRIMARY KEY (id)
	)
	`, tableName)

	_, err := t.DB.ExecContext(ctx, createTableSQL)
	if err != nil {
		msg := strings.ToLower(err.Error())
		if strings.Contains(msg, "already exists") {
			fmt.Println("Table already exists, continuing...")
		} else {
			fmt.Println("Executed create table SQL (failed): ", createTableSQL)
			return fmt.Errorf("create table failed: %w", err)
		}
	} else {
		fmt.Println("✓ Table created successfully (NO INDEXES)")
	}

	fmt.Println("✓ Table ready (no indexes - for benchmarking)")
	return nil
}

// CreateTable creates a SQL table in ImmutableDB
func (t *TableOps) CreateTable(ctx context.Context, tableName string) error {
	createTableSQL := fmt.Sprintf(`
	CREATE TABLE %s (
		id INTEGER AUTO_INCREMENT,
		transactionHash VARCHAR[66] NOT NULL,
		fromAddr VARCHAR[42] NOT NULL,
		toAddr VARCHAR[42],
		blockNumber INTEGER NOT NULL,
		blockHash VARCHAR[66] NOT NULL,
		txBlockIndex INTEGER NOT NULL,
		ts TIMESTAMP NOT NULL,
		PRIMARY KEY (id)
	)
	`, tableName)

	_, err := t.DB.ExecContext(ctx, createTableSQL)
	if err != nil {
		// Immudb returns an error if the table already exists — treat this as OK
		msg := strings.ToLower(err.Error())
		if strings.Contains(msg, "already exists") {
			fmt.Println("Table already exists, continuing...")
		} else {
			fmt.Println("Executed create table SQL (failed): ", createTableSQL)
			return fmt.Errorf("create table failed: %w", err)
		}
	} else {
		fmt.Println("✓ Table created successfully")
	}

	// Create indexes - CRITICAL: Indexes can only be added to an empty table!
	// According to ImmutableDB docs: "Index can only be added to an empty table"
	// We must check if table is empty before creating indexes

	// Check if table has any data
	rowCount, countErr := t.CountAllRecords(ctx)
	if countErr == nil && rowCount > 0 {
		fmt.Printf("\n⚠ WARNING: Table has %d records. Indexes can only be created on empty tables!\n", rowCount)
		fmt.Println("   Indexes will NOT be created. To create indexes:")
		fmt.Println("   1. Drop the table (DROP TABLE historytable)")
		fmt.Println("   2. Recreate table (this will create indexes on empty table)")
		fmt.Println("   3. Then insert data")
		fmt.Println()
		fmt.Println("   Current queries will perform full table scans (1-3s per query).")
		fmt.Println()
		return nil
	}

	// Table is empty, create indexes using correct syntax
	// ImmutableDB syntax: CREATE INDEX ON table(column) - no explicit index names
	fmt.Println("\nCreating indexes on empty table (required by ImmutableDB)...")
	indexesCreated := 0
	indexErrors := []string{}

	// Use correct ImmutableDB syntax: CREATE INDEX ON table(column)
	// No explicit index names - indexes are referenced by column list
	indexSQLs := []struct {
		name string
		sql  string
	}{
		{"transactionHash", fmt.Sprintf(`CREATE INDEX ON %s(transactionHash)`, tableName)},
		{"fromAddr", fmt.Sprintf(`CREATE INDEX ON %s(fromAddr)`, tableName)},
		{"toAddr", fmt.Sprintf(`CREATE INDEX ON %s(toAddr)`, tableName)},
		{"blockNumber", fmt.Sprintf(`CREATE INDEX ON %s(blockNumber)`, tableName)},
	}

	for _, idx := range indexSQLs {
		_, indexErr := t.DB.ExecContext(ctx, idx.sql)
		if indexErr != nil {
			errMsg := strings.ToLower(indexErr.Error())
			if strings.Contains(errMsg, "already exists") || strings.Contains(errMsg, "duplicate") {
				indexesCreated++
				fmt.Printf("✓ Index on %s already exists\n", idx.name)
			} else {
				indexErrors = append(indexErrors, fmt.Sprintf("  %s: %v", idx.name, indexErr))
				fmt.Printf("⚠ WARNING: Failed to create index on %s: %v\n", idx.name, indexErr)
				fmt.Printf("  SQL: %s\n", idx.sql)
			}
		} else {
			indexesCreated++
			fmt.Printf("✓ Index on %s created successfully\n", idx.name)
		}
	}

	// Summary
	fmt.Println()
	if len(indexErrors) > 0 {
		fmt.Printf("⚠ WARNING: %d index(es) failed to create. Queries may be slow!\n", len(indexErrors))
		fmt.Println("Failed indexes:")
		for _, err := range indexErrors {
			fmt.Println(err)
		}
	} else {
		fmt.Printf("✓ All %d indexes created successfully on empty table\n", indexesCreated)
		fmt.Println("  Indexes are now ready. You can insert data and queries should use indexes.")
		fmt.Println()
		fmt.Println("  ⚠ IMPORTANT: Based on testing, even with indexes created correctly:")
		fmt.Println("    - Queries may still be slow (600-1000ms) instead of expected (<50ms)")
		fmt.Println("    - This suggests ImmutableDB's query planner may not use indexes effectively")
		fmt.Println("    - Or indexes may need additional configuration/optimization")
		fmt.Println("    - This appears to be a limitation of ImmutableDB's SQL engine")
		fmt.Println()
		fmt.Println("  Expected performance with working indexes: <50ms for point lookups")
		fmt.Println("  If you see 600ms+, indexes are likely not being used by query planner")
	}
	fmt.Println()

	fmt.Println("✓ Table ready")
	return nil
}

// RecreateTableWithIndexes drops the existing table and recreates it with indexes
// This is necessary because ImmutableDB only allows indexes on empty tables
// WARNING: This will delete all data in the table!
func (t *TableOps) RecreateTableWithIndexes(ctx context.Context, tableName string) error {
	fmt.Println("⚠ WARNING: Dropping existing table to recreate with indexes...")
	fmt.Println("   All data will be lost!")

	// Drop table
	dropSQL := fmt.Sprintf("DROP TABLE %s", tableName)
	_, err := t.DB.ExecContext(ctx, dropSQL)
	if err != nil {
		errMsg := strings.ToLower(err.Error())
		if !strings.Contains(errMsg, "does not exist") && !strings.Contains(errMsg, "not found") {
			return fmt.Errorf("failed to drop table: %w", err)
		}
		fmt.Println("  Table didn't exist, creating new one...")
	} else {
		fmt.Println("✓ Table dropped successfully")
	}

	// Recreate table with indexes
	return t.CreateTable(ctx, tableName)
}

// DropTable drops the table (used for clean benchmarking)
func (t *TableOps) DropTable(ctx context.Context, tableName string) error {
	dropSQL := fmt.Sprintf("DROP TABLE %s", tableName)
	_, err := t.DB.ExecContext(ctx, dropSQL)
	if err != nil {
		errMsg := strings.ToLower(err.Error())
		if strings.Contains(errMsg, "does not exist") || strings.Contains(errMsg, "not found") {
			return nil // Table doesn't exist, that's fine
		}
		return fmt.Errorf("failed to drop table: %w", err)
	}
	return nil
}

// TestIndexPerformance runs a quick test to verify if indexes are actually working
func (t *TableOps) TestIndexPerformance(ctx context.Context, tableName string) error {
	fmt.Println("\n=== Testing Index Performance ===")

	// Get total record count first
	totalCount, err := t.CountAllRecords(ctx)
	if err != nil {
		return fmt.Errorf("failed to get count: %w", err)
	}

	if totalCount == 0 {
		fmt.Println("Table is empty, skipping index test")
		return nil
	}

	fmt.Printf("Table has %d records\n", totalCount)
	fmt.Println("Running diagnostic queries...")

	// Test 1: Hash query (should use index)
	fmt.Println("\n1. Testing hash query (should use index on transactionHash)...")
	testHashSQL := fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE transactionHash = ?", tableName)

	// Use a hash that likely doesn't exist to test lookup speed
	testHash := "0x0000000000000000000000000000000000000000000000000000000000000000"
	start := time.Now()
	var count int
	err = t.DB.QueryRowContext(ctx, testHashSQL, testHash).Scan(&count)
	hashDuration := time.Since(start)

	if err != nil && err != sql.ErrNoRows {
		return fmt.Errorf("hash query failed: %w", err)
	}

	fmt.Printf("   Query time: %v\n", hashDuration)
	if hashDuration > 100*time.Millisecond {
		fmt.Printf("   ⚠ WARNING: Query took %v (>100ms) - index may not be working!\n", hashDuration)
		fmt.Printf("   Expected: <50ms for indexed lookup on %d records\n", totalCount)
	} else {
		fmt.Printf("   ✓ Query is fast - index appears to be working\n")
	}

	// Test 2: FROM address query (should use index)
	fmt.Println("\n2. Testing FROM address query (should use index on fromAddr)...")
	testFromSQL := fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE fromAddr = ?", tableName)
	testAddr := "0x742d35Cc6634C0532925a3b844Bc9e7595f0bEb0"

	start = time.Now()
	err = t.DB.QueryRowContext(ctx, testFromSQL, testAddr).Scan(&count)
	fromDuration := time.Since(start)

	if err != nil {
		return fmt.Errorf("from query failed: %w", err)
	}

	fmt.Printf("   Query time: %v (found %d records)\n", fromDuration, count)
	if fromDuration > 200*time.Millisecond {
		fmt.Printf("   ⚠ WARNING: Query took %v (>200ms) - index may not be working!\n", fromDuration)
		fmt.Printf("   Expected: <100ms for indexed lookup on %d records\n", totalCount)
	} else {
		fmt.Printf("   ✓ Query is reasonably fast - index appears to be working\n")
	}

	// Summary
	fmt.Println("\n=== Index Performance Summary ===")
	if hashDuration > 100*time.Millisecond || fromDuration > 200*time.Millisecond {
		fmt.Println("⚠ WARNING: Queries are slow despite indexes being created!")
		fmt.Println("Possible causes:")
		fmt.Println("  1. ImmutableDB may not support indexes as expected")
		fmt.Println("  2. Index syntax may be incorrect for this ImmutableDB version")
		fmt.Println("  3. Indexes may need to be created differently")
		fmt.Println("  4. ImmutableDB may require explicit index usage hints")
		fmt.Println("\nRecommendation: Check ImmutableDB documentation for:")
		fmt.Println("  - Correct CREATE INDEX syntax")
		fmt.Println("  - Index support and limitations")
		fmt.Println("  - Query optimization features")
	} else {
		fmt.Println("✓ Indexes appear to be working correctly")
	}
	fmt.Println()

	return nil
}

// InsertRecord inserts a transfer record using ImmutableDB SQL
func (t *TableOps) InsertRecord(ctx context.Context, record Config.Transfer) error {
	insertRecordSQL := fmt.Sprintf(
		"INSERT INTO %s (transactionHash, fromAddr, toAddr, blockNumber, blockHash, txBlockIndex, ts) VALUES (?, ?, ?, ?, ?, ?, NOW())",
		Config.ImmuDBTable,
	)
	_, err := t.DB.ExecContext(ctx, insertRecordSQL, record.TransactionHash, record.From, record.To, record.BlockNumber, record.BlockHash, record.TxBlockIndex)
	return err
}

// InsertRecords inserts multiple transfer records in batches using ImmutableDB SQL
// Splits large batches into smaller chunks to avoid transaction limits
func (t *TableOps) InsertRecords(ctx context.Context, records []Config.Transfer) error {
	if len(records) == 0 {
		return nil
	}

	// ImmutableDB has a limit on entries per transaction, so we batch in chunks
	// Using 1000 records per batch as a safe limit
	batchSize := 200
	totalRecords := len(records)

	for i := 0; i < totalRecords; i += batchSize {
		end := i + batchSize
		if end > totalRecords {
			end = totalRecords
		}

		batch := records[i:end]
		err := t.insertBatch(ctx, batch)
		if err != nil {
			return fmt.Errorf("failed to insert batch %d-%d: %w", i, end-1, err)
		}
	}

	return nil
}

// insertBatch inserts a single batch of records
func (t *TableOps) insertBatch(ctx context.Context, records []Config.Transfer) error {
	if len(records) == 0 {
		return nil
	}

	// Build batch INSERT statement with multiple VALUES clauses
	insertRecordsSQL := fmt.Sprintf(
		"INSERT INTO %s (transactionHash, fromAddr, toAddr, blockNumber, blockHash, txBlockIndex, ts) VALUES ",
		Config.ImmuDBTable,
	)

	// Build VALUES placeholders and arguments
	// Using NOW() for ts column instead of parameterized value
	values := make([]string, 0, len(records))
	args := make([]interface{}, 0, len(records)*6) // 6 fields per record (ts uses NOW())

	for _, record := range records {
		values = append(values, "(?, ?, ?, ?, ?, ?, NOW())")
		args = append(args, record.TransactionHash, record.From, record.To, record.BlockNumber, record.BlockHash, record.TxBlockIndex)
	}

	insertRecordsSQL += strings.Join(values, ", ")

	// Execute batch insert
	_, err := t.DB.ExecContext(ctx, insertRecordsSQL, args...)
	if err != nil {
		return fmt.Errorf("failed to insert batch: %w", err)
	}

	return nil
}

// QueryRecord retrieves a transfer record by transactionHash using ImmutableDB SQL
// The index on transactionHash will be used automatically by the database
func (t *TableOps) QueryRecord(ctx context.Context, transactionHash string) (*Config.Transfer, error) {
	// Try to use index hint if ImmutableDB supports it
	// Note: ImmutableDB may not support index hints, but worth trying
	queryRecordSQL := fmt.Sprintf(
		"SELECT transactionHash, fromAddr, toAddr, blockNumber, blockHash, txBlockIndex, ts FROM %s WHERE transactionHash = ?",
		Config.ImmuDBTable,
	)

	var record Config.Transfer
	var ts time.Time
	err := t.DB.QueryRowContext(ctx, queryRecordSQL, transactionHash).Scan(
		&record.TransactionHash,
		&record.From,
		&record.To,
		&record.BlockNumber,
		&record.BlockHash,
		&record.TxBlockIndex,
		&ts,
	)
	if err == nil {
		record.Timestamp = ts.Unix() // Convert time.Time to Unix timestamp
	}
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil // Record not found
		}
		return nil, fmt.Errorf("failed to query record: %w", err)
	}

	return &record, nil
}

// QueryRecordsByFrom retrieves all records by From address using ImmutableDB SQL
// Returns a slice of Transfer records as there can be multiple transactions from the same address
func (t *TableOps) QueryRecordsByFrom(ctx context.Context, fromAddress string) ([]*Config.Transfer, error) {
	queryRecordsByFromSQL := fmt.Sprintf(
		"SELECT transactionHash, fromAddr, toAddr, blockNumber, blockHash, txBlockIndex, ts FROM %s WHERE fromAddr = ?",
		Config.ImmuDBTable,
	)

	rows, err := t.DB.QueryContext(ctx, queryRecordsByFromSQL, fromAddress)
	if err != nil {
		return nil, fmt.Errorf("failed to query records: %w", err)
	}
	defer rows.Close()

	var records []*Config.Transfer
	for rows.Next() {
		var record Config.Transfer
		var ts time.Time
		err := rows.Scan(
			&record.TransactionHash,
			&record.From,
			&record.To,
			&record.BlockNumber,
			&record.BlockHash,
			&record.TxBlockIndex,
			&ts,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan record: %w", err)
		}
		record.Timestamp = ts.Unix() // Convert time.Time to Unix timestamp
		records = append(records, &record)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating rows: %w", err)
	}

	return records, nil
}

// QueryRecordsByTo retrieves all records by To address using ImmutableDB SQL
// Returns a slice of Transfer records as there can be multiple transactions to the same address
func (t *TableOps) QueryRecordsByTo(ctx context.Context, toAddress string) ([]*Config.Transfer, error) {
	queryRecordsByToSQL := fmt.Sprintf(
		"SELECT transactionHash, fromAddr, toAddr, blockNumber, blockHash, txBlockIndex, ts FROM %s WHERE toAddr = ?",
		Config.ImmuDBTable,
	)
	rows, err := t.DB.QueryContext(ctx, queryRecordsByToSQL, toAddress)
	if err != nil {
		return nil, fmt.Errorf("failed to query records: %w", err)
	}
	defer rows.Close()

	var records []*Config.Transfer
	for rows.Next() {
		var record Config.Transfer
		var ts time.Time
		err := rows.Scan(
			&record.TransactionHash,
			&record.From,
			&record.To,
			&record.BlockNumber,
			&record.BlockHash,
			&record.TxBlockIndex,
			&ts,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan record: %w", err)
		}
		record.Timestamp = ts.Unix() // Convert time.Time to Unix timestamp
		records = append(records, &record)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating rows: %w", err)
	}

	return records, nil
}

// QueryRecordsByBlockNumber retrieves all records by Block Number using ImmutableDB SQL
// Returns a slice of Transfer records as there can be multiple transactions at the same block number
func (t *TableOps) QueryRecordsByBlockNumber(ctx context.Context, blockNumber int) ([]*Config.Transfer, error) {
	queryRecordsByBlockNumberSQL := fmt.Sprintf(
		"SELECT transactionHash, fromAddr, toAddr, blockNumber, blockHash, txBlockIndex, ts FROM %s WHERE blockNumber = ?",
		Config.ImmuDBTable,
	)
	rows, err := t.DB.QueryContext(ctx, queryRecordsByBlockNumberSQL, blockNumber)
	if err != nil {
		return nil, fmt.Errorf("failed to query records: %w", err)
	}
	defer rows.Close()

	var records []*Config.Transfer
	for rows.Next() {
		var record Config.Transfer
		var ts time.Time
		err := rows.Scan(
			&record.TransactionHash,
			&record.From,
			&record.To,
			&record.BlockNumber,
			&record.BlockHash,
			&record.TxBlockIndex,
			&ts,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan record: %w", err)
		}
		record.Timestamp = ts.Unix() // Convert time.Time to Unix timestamp
		records = append(records, &record)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating rows: %w", err)
	}

	return records, nil
}

// CountRecords counts the number of records for a given from address using ImmutableDB SQL
func (t *TableOps) CountRecords(ctx context.Context, fromAddress string) (int, error) {
	countRecordsSQL := fmt.Sprintf(
		"SELECT COUNT(*) FROM %s WHERE fromAddr = ?",
		Config.ImmuDBTable,
	)
	var count int
	err := t.DB.QueryRowContext(ctx, countRecordsSQL, fromAddress).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("failed to count records: %w", err)
	}
	return count, nil
}

// CountRecordsTo counts the number of records for a given to address using ImmutableDB SQL
func (t *TableOps) CountRecordsTo(ctx context.Context, toAddress string) (int, error) {
	countRecordsSQL := fmt.Sprintf(
		"SELECT COUNT(*) FROM %s WHERE toAddr = ?",
		Config.ImmuDBTable,
	)
	var count int
	err := t.DB.QueryRowContext(ctx, countRecordsSQL, toAddress).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("failed to count records: %w", err)
	}
	return count, nil
}

// Count of all records in the table
func (t *TableOps) CountAllRecords(ctx context.Context) (int, error) {
	countRecordsSQL := fmt.Sprintf(
		"SELECT COUNT(*) FROM %s",
		Config.ImmuDBTable,
	)
	var count int
	err := t.DB.QueryRowContext(ctx, countRecordsSQL).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("failed to count records: %w", err)
	}
	return count, nil
}

// GetTailRecord retrieves the last inserted record (highest ID) for O(1) lookup
// Since ID is AUTO_INCREMENT, the tail record has the maximum ID
func (t *TableOps) GetTailRecord(ctx context.Context) (*Config.Transfer, int64, error) {
	getTailSQL := fmt.Sprintf(
		"SELECT id, transactionHash, fromAddr, toAddr, blockNumber, blockHash, txBlockIndex, ts FROM %s ORDER BY id DESC LIMIT 1",
		Config.ImmuDBTable,
	)

	var record Config.Transfer
	var id int64
	var ts time.Time
	err := t.DB.QueryRowContext(ctx, getTailSQL).Scan(
		&id,
		&record.TransactionHash,
		&record.From,
		&record.To,
		&record.BlockNumber,
		&record.BlockHash,
		&record.TxBlockIndex,
		&ts,
	)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, 0, nil // No records found
		}
		return nil, 0, fmt.Errorf("failed to get tail record: %w", err)
	}

	record.Timestamp = ts.Unix() // Convert time.Time to Unix timestamp
	return &record, id, nil
}

// GetHeadRecord retrieves the first inserted record (lowest ID) for O(1) lookup
func (t *TableOps) GetHeadRecord(ctx context.Context) (*Config.Transfer, int64, error) {
	getHeadSQL := fmt.Sprintf(
		"SELECT id, transactionHash, fromAddr, toAddr, blockNumber, blockHash, txBlockIndex, ts FROM %s ORDER BY id ASC LIMIT 1",
		Config.ImmuDBTable,
	)

	var record Config.Transfer
	var id int64
	var ts time.Time
	err := t.DB.QueryRowContext(ctx, getHeadSQL).Scan(
		&id,
		&record.TransactionHash,
		&record.From,
		&record.To,
		&record.BlockNumber,
		&record.BlockHash,
		&record.TxBlockIndex,
		&ts,
	)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, 0, nil // No records found
		}
		return nil, 0, fmt.Errorf("failed to get head record: %w", err)
	}

	record.Timestamp = ts.Unix() // Convert time.Time to Unix timestamp
	return &record, id, nil
}

// GetSampleRecords retrieves a sample of records from the table
func (t *TableOps) GetSampleRecords(ctx context.Context, limit int) ([]*Config.Transfer, error) {
	getSampleSQL := fmt.Sprintf(
		"SELECT transactionHash, fromAddr, toAddr, blockNumber, blockHash, txBlockIndex, ts FROM %s ORDER BY id ASC LIMIT ?",
		Config.ImmuDBTable,
	)

	rows, err := t.DB.QueryContext(ctx, getSampleSQL, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to query sample records: %w", err)
	}
	defer rows.Close()

	var records []*Config.Transfer
	for rows.Next() {
		var record Config.Transfer
		var ts time.Time
		err := rows.Scan(
			&record.TransactionHash,
			&record.From,
			&record.To,
			&record.BlockNumber,
			&record.BlockHash,
			&record.TxBlockIndex,
			&ts,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan record: %w", err)
		}
		record.Timestamp = ts.Unix() // Convert time.Time to Unix timestamp
		records = append(records, &record)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating rows: %w", err)
	}

	return records, nil
}

// GetTableStatistics retrieves aggregate statistics about the table
type TableStatistics struct {
	TotalRecords    int
	MinBlockNumber  int
	MaxBlockNumber  int
	MinTimestamp    int64
	MaxTimestamp    int64
	UniqueFromAddrs int
	UniqueToAddrs   int
}

func (t *TableOps) GetTableStatistics(ctx context.Context) (*TableStatistics, error) {
	stats := &TableStatistics{}

	// Get total count
	totalCount, err := t.CountAllRecords(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get total count: %w", err)
	}
	stats.TotalRecords = totalCount

	if totalCount == 0 {
		return stats, nil
	}

	// Get min/max block number
	minMaxBlockSQL := fmt.Sprintf(
		"SELECT MIN(blockNumber), MAX(blockNumber) FROM %s",
		Config.ImmuDBTable,
	)
	err = t.DB.QueryRowContext(ctx, minMaxBlockSQL).Scan(&stats.MinBlockNumber, &stats.MaxBlockNumber)
	if err != nil {
		return nil, fmt.Errorf("failed to get block number range: %w", err)
	}

	// Get min/max timestamp
	minMaxTimeSQL := fmt.Sprintf(
		"SELECT MIN(ts), MAX(ts) FROM %s",
		Config.ImmuDBTable,
	)
	var minTime, maxTime time.Time
	err = t.DB.QueryRowContext(ctx, minMaxTimeSQL).Scan(&minTime, &maxTime)
	if err != nil {
		return nil, fmt.Errorf("failed to get timestamp range: %w", err)
	}
	stats.MinTimestamp = minTime.Unix()
	stats.MaxTimestamp = maxTime.Unix()

	// Get unique from addresses count
	// ImmutableDB may not support COUNT(DISTINCT), so we'll query and count manually
	uniqueFromSQL := fmt.Sprintf(
		"SELECT fromAddr FROM %s GROUP BY fromAddr",
		Config.ImmuDBTable,
	)
	rows, err := t.DB.QueryContext(ctx, uniqueFromSQL)
	if err != nil {
		// If GROUP BY doesn't work, skip unique count
		stats.UniqueFromAddrs = -1
	} else {
		defer rows.Close()
		count := 0
		for rows.Next() {
			count++
		}
		stats.UniqueFromAddrs = count
	}

	// Get unique to addresses count
	uniqueToSQL := fmt.Sprintf(
		"SELECT toAddr FROM %s GROUP BY toAddr",
		Config.ImmuDBTable,
	)
	rows, err = t.DB.QueryContext(ctx, uniqueToSQL)
	if err != nil {
		// If GROUP BY doesn't work, skip unique count
		stats.UniqueToAddrs = -1
	} else {
		defer rows.Close()
		count := 0
		for rows.Next() {
			count++
		}
		stats.UniqueToAddrs = count
	}

	return stats, nil
}
