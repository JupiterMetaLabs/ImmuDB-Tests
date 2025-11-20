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
        "from" VARCHAR[42] NOT NULL,
        "to" VARCHAR[42],
		txIndex INTEGER NOT NULL,
        blockNumber INTEGER NOT NULL,
        transactionHash VARCHAR[66] NOT NULL,
        ts TIMESTAMP NOT NULL,
        PRIMARY KEY (id)
    )
	`, tableName)

	_, err := t.DB.ExecContext(ctx, createTableSQL)
	if err != nil {
		fmt.Println("Executed create table SQL (failed): ", createTableSQL)
		return err
	}

	// Create index on transactionHash
	createIndexSQL := fmt.Sprintf(
		"CREATE INDEX IF NOT EXISTS idx_txhash ON %s(transactionHash)",
		tableName,
	)
	_, err = t.DB.ExecContext(ctx, createIndexSQL)
	if err != nil {
		return err
	}

	//create index on from
	createIndexSQL = fmt.Sprintf(
		"CREATE INDEX IF NOT EXISTS idx_from ON %s(\"from\")",
		tableName,
	)
	_, err = t.DB.ExecContext(ctx, createIndexSQL)
	if err != nil {
		return err
	}

	fmt.Println("✓ Table created successfully")
	fmt.Println("Executed create table SQL: ", createTableSQL)
	return nil
}

// InsertRecord inserts a transfer record using ImmutableDB SQL
func (t *TableOps) InsertRecord(ctx context.Context, record Config.Transfer) error {
	insertRecordSQL := fmt.Sprintf(
		"INSERT INTO %s (\"from\", \"to\", txIndex, blockNumber, transactionHash, ts) VALUES (?, ?, ?, ?, ?, NOW())",
		Config.ImmuDBTable,
	)
	_, err := t.DB.ExecContext(ctx, insertRecordSQL, record.From, record.To, record.BlockNumber, record.TransactionHash)
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
	batchSize := 1000
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
		"INSERT INTO %s (\"from\", \"to\", txIndex, blockNumber, transactionHash, ts) VALUES ",
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
		return fmt.Errorf("failed to insert batch: %w", err)
	}

	return nil
}

// QueryRecord retrieves a transfer record by transactionHash using ImmutableDB SQL
// The index on transactionHash will be used automatically by the database
func (t *TableOps) QueryRecord(ctx context.Context, transactionHash string) (*Config.Transfer, error) {
	queryRecordSQL := fmt.Sprintf(
		"SELECT \"from\", \"to\", txIndex, blockNumber, transactionHash, ts FROM %s WHERE transactionHash = ?",
		Config.ImmuDBTable,
	)

	var record Config.Transfer
	var ts time.Time
	err := t.DB.QueryRowContext(ctx, queryRecordSQL, transactionHash).Scan(
		&record.From,
		&record.To,
		&record.TxIndex,
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
		"SELECT \"from\", \"to\", txIndex, blockNumber, transactionHash, ts FROM %s WHERE \"from\" = ?",
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
			&record.TxIndex,
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
		"SELECT \"from\", \"to\", txIndex, blockNumber, transactionHash, ts FROM %s WHERE \"to\" = ?",
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
			&record.TxIndex,
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
		"SELECT \"from\", \"to\", txIndex, blockNumber, transactionHash, ts FROM %s WHERE blockNumber = ?",
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
			&record.TxIndex,
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
		"SELECT id FROM %s ORDER BY id DESC LIMIT 1",
		Config.ImmuDBTable,
	)

	var record Config.Transfer
	var id int64
	var ts time.Time
	err := t.DB.QueryRowContext(ctx, getTailSQL).Scan(
		&id,
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
