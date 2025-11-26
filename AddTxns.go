package main

import (
	"DBTests/IMMUSQL"
	"context"
	"fmt"
)

// This function is just to append the simulator transactions to the DB
func AddtransactionsToDB() {

	// Get the transaction from the function generateBlockBasedTransactions
	transactions := generateBlockBasedTransactions(100000, 200, 50)
	tableOps := IMMUSQL.GetTableOps()
	// Add the transactions to the DB
	err := tableOps.InsertRecords(context.Background(), transactions)
	if err != nil {
		fmt.Printf("Failed to add transactions to the DB: %v\n", err)
		return
	}

	fmt.Println("Transactions added to the DB successfully. Printing Head 5 and Tail 5 transactions:")
	// Print the first 5 and last 5 transactions
	for i := 0; i < 5; i++ {
		fmt.Printf("Transaction %d: %+v\n", i+1, transactions[i])
	}
	for i := len(transactions) - 5; i < len(transactions); i++ {
		fmt.Printf("Transaction %d: %+v\n", i+1, transactions[i])
	}
}