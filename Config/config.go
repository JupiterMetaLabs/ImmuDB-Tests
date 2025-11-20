package Config

const (
	ImmuDBHost     = "localhost"
	ImmuDBPort     = 3322
	ImmuDBUser     = "immudb"
	ImmuDBPassword = "immudb"
	ImmuDBDatabase = "historydb"
	ImmuDBTable    = "historytable"
)

type Transfer struct {
	From            string `json:"from"`
	To              string `json:"to"`
	BlockNumber     int    `json:"blockNumber"`
	TransactionHash string `json:"transactionHash"`
	Timestamp       int64  `json:"timestamp"`
	TxIndex         int    `json:"txIndex"`
}
