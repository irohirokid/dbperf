package cloudspanner

import (
	"context"

	"cloud.google.com/go/spanner"
	"github.com/irohirokid/dbperf/db"
	"google.golang.org/api/option"
)

type AppSpanner struct {
	client *spanner.Client
}

func NewClient(db_id string, connPoolSize int) (db.Client, error) {
	client, err := spanner.NewClient(context.Background(), db_id, option.WithGRPCConnectionPool(connPoolSize))
	if err != nil {
		return nil, err
	}
	return &AppSpanner{client}, nil
}
