package cloudspanner

import (
	"context"
	"errors"
	"time"

	"cloud.google.com/go/spanner"
	"github.com/irohiroki/spanner-performance-test/configs"
)

func (appSpanner AppSpanner) MeasureTransaction() (time.Duration, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
	defer cancel()

	start := time.Now()
	_, err := appSpanner.client.ReadWriteTransaction(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
		userId := configs.RandUserId()
		userRow, err := txn.ReadRow(ctx, "Users", spanner.Key{userId}, []string{"Gold"})
		if err != nil {
			return err
		}

		var gold int64
		err = userRow.Column(0, &gold)
		if err != nil {
			return err
		}

		if gold < 5 {
			return errors.New("not enough gold")
		}

		var userItemRow *spanner.Row
		userItemRow, err = txn.ReadRow(ctx, "UserItems", spanner.Key{userId}, []string{"Amount"})
		if err != nil {
			return err
		}

		var amount int64
		err = userItemRow.Column(0, &amount)
		if err != nil {
			return err
		}

		err = txn.BufferWrite([]*spanner.Mutation{
			spanner.Update("Users", []string{"Id", "Gold"}, []interface{}{1, gold - 5}),
			spanner.Update("UserItems", []string{"Id", "Amount"}, []interface{}{1, amount + 1}),
		})
		return err
	})
	return time.Since(start), err
}
