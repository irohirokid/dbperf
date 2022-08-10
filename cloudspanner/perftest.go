package cloudspanner

import (
	"context"
	"errors"
	"fmt"
	"time"

	"cloud.google.com/go/spanner"
	"github.com/irohirokid/dbperf/configs"
)

func (s AppSpanner) ConsistentRead() error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*99/10)
	defer cancel()

	userId := spannerKey(configs.RandUserId())

	_, err := s.client.ReadWriteTransaction(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
		stmt := spanner.Statement{
			SQL: fmt.Sprintf("UPDATE Users SET Gold = Gold + 5 WHERE Id = %d", userId),
		}
		_, err := txn.Update(ctx, stmt)
		return err
	})
	if err != nil {
		return err
	}

	row, err := s.client.Single().ReadRow(ctx, "Users", spanner.Key{userId}, []string{"Gold"})
	if err != nil {
		return err
	}

	var gold int64
	err = row.ColumnByName("Gold", &gold)
	return err
}

func (s AppSpanner) SimpleRead() error {
	ro := s.client.ReadOnlyTransaction().WithTimestampBound(spanner.ExactStaleness(15 * time.Second))
	defer ro.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*99/10)
	defer cancel()
	row, err := ro.ReadRow(ctx, "Users", spanner.Key{spannerKey(configs.RandUserId())}, []string{"Gold"})
	if err != nil {
		return err
	}

	var gold int64
	err = row.ColumnByName("Gold", &gold)
	return err
}

func (s AppSpanner) TransactWrite() error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*99/10)
	defer cancel()

	_, err := s.client.ReadWriteTransaction(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
		userId := spannerKey(configs.RandUserId())
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
