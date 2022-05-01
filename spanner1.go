package main

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"time"

	"cloud.google.com/go/spanner"
)

type AppSpanner struct {
	client *spanner.Client
}

func (appSpanner AppSpanner) populate(start int, end int) error {
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
	defer cancel()

	_, err := appSpanner.client.ReadWriteTransaction(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
		b := bytes.NewBufferString("INSERT Users (Id, Gold) VALUES ")
		for i := start; i < end; i++ {
			b.WriteString(fmt.Sprintf("(%d,10000),", i))
		}
		b.WriteString(fmt.Sprintf("(%d,10000)", end))
		stmt := spanner.Statement{
			SQL: b.String(),
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

func (appSpanner AppSpanner) populateMany(numUsers int) error {
	for startPos, endPos := 1, 0; endPos < numUsers; {
		if endPos+50000 < numUsers {
			endPos += 50000
		} else {
			endPos = numUsers
		}

		err := appSpanner.populate(startPos, endPos)
		if err != nil {
			return err
		}

		startPos = endPos + 1
	}
	return nil
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

	err = appClient.populateMany(100000)
	if err != nil {
		fmt.Println(err)
	}
}
