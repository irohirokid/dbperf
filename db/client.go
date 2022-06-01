package db

type Client interface {
	ConsistentRead() error
	SimpleRead() error
	TransactWrite() error
	PopulateMany(numUsers int) error
}
