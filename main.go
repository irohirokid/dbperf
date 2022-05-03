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
	err := appSpanner.populateTable(start, end, "Users (Id, Gold)", "(%d,10000)")
	if err != nil {
		return err
	}

	err = appSpanner.populateTable(start, end, "UserItems (Id, Amount)", "(%d,0)")
	if err != nil {
		return err
	}
	return nil
}

func (appSpanner AppSpanner) populateTable(start int, end int, columns string, rowTmpl string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
	defer cancel()

	_, err := appSpanner.client.ReadWriteTransaction(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
		b := bytes.NewBufferString("INSERT " + columns + " VALUES ")
		for i := start; i < end; i++ {
			b.WriteString(fmt.Sprintf(rowTmpl+",", i))
		}
		b.WriteString(fmt.Sprintf(rowTmpl, end))
		stmt := spanner.Statement{
			SQL: b.String(),
		}
		rowCount, err := txn.Update(ctx, stmt)
		if err != nil {
			return err
		}
		fmt.Fprintf(os.Stdout, "%s: %d record(s) inserted.\n", columns, rowCount)
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
