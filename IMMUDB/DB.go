package IMMUDB

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync"

	"github.com/codenotary/immudb/pkg/client"
	"github.com/codenotary/immudb/pkg/stdlib"

	"DBTests/Config"
)

var (
	db   *sql.DB
	once sync.Once
	err  error
)

// createDatabaseIfNotExists creates the database if it doesn't exist
func createDatabaseIfNotExists(ctx context.Context, dbName string) error {
	// First connect to defaultdb to create the target database
	defaultOpts := client.DefaultOptions()
	defaultOpts.Address = Config.ImmuDBHost
	defaultOpts.Port = Config.ImmuDBPort
	defaultOpts.Username = Config.ImmuDBUser
	defaultOpts.Password = Config.ImmuDBPassword
	defaultOpts.Database = "defaultdb" // Connect to defaultdb first

	defaultDB := stdlib.OpenDB(defaultOpts)
	defer defaultDB.Close()

	// Try to create the database
	createDBQuery := fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", dbName)
	_, err := defaultDB.ExecContext(ctx, createDBQuery)
	if err != nil {
		// If database already exists, that's fine
		errMsg := strings.ToLower(err.Error())
		if strings.Contains(errMsg, "already exists") ||
			strings.Contains(errMsg, "database exists") ||
			strings.Contains(errMsg, "could not be created") {
			// Database already exists, no error - this is expected
			return nil
		}
		return fmt.Errorf("failed to create database: %w", err)
	}
	return nil
}

// ConnectDB creates and returns a singleton SQL database connection to ImmutableDB using configuration from Config package
// This uses the native client connection internally via stdlib
// It will create the database if it doesn't exist
func ConnectDB() (*sql.DB, error) {
	once.Do(func() {
		// Debugging: Print the connection details
		fmt.Printf("Connecting to ImmutableDB at %s:%d\n", Config.ImmuDBHost, Config.ImmuDBPort)
		fmt.Printf("Username: %s\n", Config.ImmuDBUser)
		fmt.Printf("Database: %s\n", Config.ImmuDBDatabase)

		ctx := context.Background()

		// Create database if it doesn't exist
		fmt.Printf("Creating database '%s' if it doesn't exist...\n", Config.ImmuDBDatabase)
		if err = createDatabaseIfNotExists(ctx, Config.ImmuDBDatabase); err != nil {
			return
		}

		opts := client.DefaultOptions()
		opts.Address = Config.ImmuDBHost
		opts.Port = Config.ImmuDBPort
		opts.Username = Config.ImmuDBUser
		opts.Password = Config.ImmuDBPassword
		opts.Database = Config.ImmuDBDatabase

		// Use stdlib to get *sql.DB which internally uses the native client
		db = stdlib.OpenDB(opts)

		// Test the connection
		if err = db.Ping(); err != nil {
			db.Close()
			db = nil
			return
		}
		fmt.Println("✓ Successfully connected to ImmutableDB")
	})
	return db, err
}
