package cloudspanner

import (
	"context"
	"time"

	"cloud.google.com/go/spanner"
	"github.com/irohirokid/dbperf/db"
)

type AppSpanner struct {
	client *spanner.Client
}

func NewClient(db_id string) (db.Client, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
	defer cancel()

	client, err := spanner.NewClient(ctx, db_id)
	if err != nil {
		return nil, err
	}
	return &AppSpanner{client}, nil
}
