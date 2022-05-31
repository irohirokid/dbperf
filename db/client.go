package db

type Client interface {
	SimpleRead() error
	TransactWrite() error
	PopulateMany(numUsers int) error
}
