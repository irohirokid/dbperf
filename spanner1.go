package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"cloud.google.com/go/spanner"
)

type AppSpanner struct {
	client *spanner.Client
}

func (appSpanner AppSpanner) populate(numUsers int) error {
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
	defer cancel()

	_, err := appSpanner.client.ReadWriteTransaction(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
		stmt := spanner.Statement{
			SQL: `INSERT Users (Id, Gold) VALUES
                                (1, 10000),
                                (2, 10000)`,
		}
		rowCount, err := txn.Update(ctx, stmt)
		if err != nil {
			return err
		}
		fmt.Fprintf(os.Stdout, "%d record(s) inserted.\n", rowCount)
		return err
	})
	return err
}

func NewAppSpannerClient() (*AppSpanner, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
	defer cancel()

	client, err := spanner.NewClient(ctx, "projects/your-project-id/instances/test-instance/databases/main")
	if err != nil {
		return nil, err
	}
	return &AppSpanner{client}, nil
}

func main() {
	appClient, err := NewAppSpannerClient()
	if err != nil {
		fmt.Println(err)
	}

	err = appClient.populate(1)
	if err != nil {
		fmt.Println(err)
	}
}
