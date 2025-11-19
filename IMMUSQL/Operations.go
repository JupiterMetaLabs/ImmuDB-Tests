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

// CreateTable creates a SQL table in ImmutableDB
func (t *TableOps) CreateTable(ctx context.Context, tableName string) error {
	// Create table with id as AUTO_INCREMENT PRIMARY KEY
	createTableSQL := fmt.Sprintf(`
    CREATE TABLE IF NOT EXISTS %s (
        id INTEGER AUTO_INCREMENT,
        "from" VARCHAR[256],
        "to" VARCHAR[256],
        blockNumber INTEGER,
        transactionHash VARCHAR[66],
        ts TIMESTAMP,
        PRIMARY KEY (id)
    )
	`, tableName)

	_, err := t.DB.ExecContext(ctx, createTableSQL)
	if err != nil {
		fmt.Println("Executed create table SQL (failed): ", createTableSQL)
		return err
	}

	// Create index on transactionHash for faster lookups
	// Note: ImmutableDB index syntax may differ - using standard SQL syntax
	createIndexSQL := fmt.Sprintf("CREATE INDEX IF NOT EXISTS idx_txhash ON %s(transactionHash)", tableName)
	_, err = t.DB.ExecContext(ctx, createIndexSQL)
	if err != nil {
		// Index might not be supported or already exist, log but don't fail
		fmt.Printf("Note: Could not create index (may not be supported or already exist): %v\n", err)
	} else {
		fmt.Println("✓ Index created successfully")
	}

	fmt.Println("✓ Table created successfully")
	fmt.Println("Executed create table SQL: ", createTableSQL)
	return nil
}

// InsertRecord inserts a transfer record using ImmutableDB SQL
func (t *TableOps) InsertRecord(ctx context.Context, record Config.Transfer) error {
	insertRecordSQL := fmt.Sprintf(
		"INSERT INTO %s (\"from\", \"to\", blockNumber, transactionHash, ts) VALUES (?, ?, ?, ?, NOW())",
		Config.ImmuDBTable,
	)
	_, err := t.DB.ExecContext(ctx, insertRecordSQL, record.From, record.To, record.BlockNumber, record.TransactionHash)
	return err
}

// InsertRecords inserts multiple transfer records in a single batch operation using ImmutableDB SQL
// This is more efficient than inserting records one by one
func (t *TableOps) InsertRecords(ctx context.Context, records []Config.Transfer) error {
	if len(records) == 0 {
		return nil
	}

	// Build batch INSERT statement with multiple VALUES clauses
	insertRecordsSQL := fmt.Sprintf(
		"INSERT INTO %s (\"from\", \"to\", blockNumber, transactionHash, ts) VALUES ",
		Config.ImmuDBTable,
	)

	// Build VALUES placeholders and arguments
	// Using NOW() for ts column instead of parameterized value
	values := make([]string, 0, len(records))
	args := make([]interface{}, 0, len(records)*4) // 4 fields per record (ts uses NOW())

	for _, record := range records {
		values = append(values, "(?, ?, ?, ?, NOW())")
		args = append(args, record.From, record.To, record.BlockNumber, record.TransactionHash)
	}

	insertRecordsSQL += strings.Join(values, ", ")

	// Execute batch insert
	_, err := t.DB.ExecContext(ctx, insertRecordsSQL, args...)
	if err != nil {
		return fmt.Errorf("failed to insert records: %w", err)
	}

	return nil
}

// QueryRecord retrieves a transfer record by transactionHash using ImmutableDB SQL
func (t *TableOps) QueryRecord(ctx context.Context, transactionHash string) (*Config.Transfer, error) {
	queryRecordSQL := fmt.Sprintf(
		"SELECT \"from\", \"to\", blockNumber, transactionHash, ts FROM %s WHERE transactionHash = ?",
		Config.ImmuDBTable,
	)

	var record Config.Transfer
	var ts time.Time
	err := t.DB.QueryRowContext(ctx, queryRecordSQL, transactionHash).Scan(
		&record.From,
		&record.To,
		&record.BlockNumber,
		&record.TransactionHash,
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
		"SELECT \"from\", \"to\", blockNumber, transactionHash, ts FROM %s WHERE \"from\" = ?",
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
			&record.From,
			&record.To,
			&record.BlockNumber,
			&record.TransactionHash,
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
		"SELECT \"from\", \"to\", blockNumber, transactionHash, ts FROM %s WHERE \"to\" = ?",
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
			&record.From,
			&record.To,
			&record.BlockNumber,
			&record.TransactionHash,
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
		"SELECT \"from\", \"to\", blockNumber, transactionHash, ts FROM %s WHERE blockNumber = ?",
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
			&record.From,
			&record.To,
			&record.BlockNumber,
			&record.TransactionHash,
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
		"SELECT COUNT(*) FROM %s WHERE \"from\" = ?",
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
		"SELECT COUNT(*) FROM %s WHERE \"to\" = ?",
		Config.ImmuDBTable,
	)
	var count int
	err := t.DB.QueryRowContext(ctx, countRecordsSQL, toAddress).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("failed to count records: %w", err)
	}
	return count, nil
}
