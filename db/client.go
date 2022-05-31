package db

type Client interface {
	TransactWrite() error
	PopulateMany(numUsers int) error
}
