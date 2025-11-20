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
	BlockHash       string `json:"blockHash"`
	TxBlockIndex    int    `json:"txBlockIndex"`
	Timestamp       int64  `json:"timestamp"`
}
