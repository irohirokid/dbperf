package cloudspanner

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"time"

	"cloud.google.com/go/spanner"
)

func (s AppSpanner) populate(start int, end int) error {
	err := s.populateTable(start, end, "Users (Id, Gold)", "(%d,10000)")
	if err != nil {
		return err
	}

	err = s.populateTable(start, end, "UserItems (Id, Amount)", "(%d,0)")
	if err != nil {
		return err
	}
	return nil
}

func (s AppSpanner) PopulateMany(numUsers int, populationBatchSize int) error {
	for startPos, endPos := 1, 0; endPos < numUsers; {
		if endPos+populationBatchSize < numUsers {
			endPos += populationBatchSize
		} else {
			endPos = numUsers
		}

		err := s.populate(startPos, endPos)
		if err != nil {
			return err
		}

		startPos = endPos + 1
	}
	return nil
}

func (s AppSpanner) populateTable(start int, end int, columns string, rowTmpl string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
	defer cancel()

	_, err := s.client.ReadWriteTransaction(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
		b := bytes.NewBufferString("INSERT " + columns + " VALUES ")
		for i := start; i < end; i++ {
			b.WriteString(fmt.Sprintf(rowTmpl+",", spannerKey(uint32(i))))
		}
		b.WriteString(fmt.Sprintf(rowTmpl, spannerKey(uint32(end))))
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
