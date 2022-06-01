package cloudspanner

import (
	"context"
	"errors"
	"fmt"
	"time"

	"cloud.google.com/go/spanner"
	"github.com/irohirokid/dbperf/configs"
)

func (appSpanner AppSpanner) ConsistentRead() error {
	ctx := context.Background()
	userId := configs.RandUserId()

	_, err := appSpanner.client.ReadWriteTransaction(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
		stmt := spanner.Statement{
			SQL: fmt.Sprintf("UPDATE Users SET Gold = Gold + 5 WHERE Id = %d", userId),
		}
		_, err := txn.Update(ctx, stmt)
		return err
	})
	if err != nil {
		return err
	}

	row, err := appSpanner.client.Single().ReadRow(ctx, "Users", spanner.Key{userId}, []string{"Gold"})
	if err != nil {
		return err
	}

	var gold int64
	err = row.ColumnByName("Gold", &gold)
	return err
}

func (appSpanner AppSpanner) SimpleRead() error {
	ro := appSpanner.client.ReadOnlyTransaction().WithTimestampBound(spanner.ExactStaleness(15 * time.Second))
	defer ro.Close()

	ctx := context.Background()
	row, err := ro.ReadRow(ctx, "Users", spanner.Key{configs.RandUserId()}, []string{"Gold"})
	if err != nil {
		return err
	}

	var gold int64
	err = row.ColumnByName("Gold", &gold)
	return err
}

func (appSpanner AppSpanner) TransactWrite() error {
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
	defer cancel()

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
	return err
}
