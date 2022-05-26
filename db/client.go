package db

import "time"

type Client interface {
	MeasureTransaction() (time.Duration, error)
	PopulateMany(numUsers int) error
}
