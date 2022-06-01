package cloudspanner

import (
	"context"

	"cloud.google.com/go/spanner"
	"github.com/irohirokid/dbperf/db"
)

type AppSpanner struct {
	client *spanner.Client
}

func NewClient(db_id string) (db.Client, error) {
	client, err := spanner.NewClient(context.Background(), db_id)
	if err != nil {
		return nil, err
	}
	return &AppSpanner{client}, nil
}
