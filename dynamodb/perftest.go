package dynamodb

import (
	"fmt"
	"os"
	"regexp"

	"github.com/guregu/dynamo"
	"github.com/irohirokid/dbperf/configs"
)

func (d AppDynamoDB) ConsistentRead() error {
	userKey := fmt.Sprintf("User%d", configs.RandUserId())

	update := d.client.Table("Main").Update("PK", userKey).Range("SK", userKey).Add("Gold", 5)
	err := d.client.WriteTx().Update(update).Run()
	if err != nil {
		matched, regexperr := regexp.MatchString("TransactionConflict", err.Error())
		if regexperr != nil {
			fmt.Fprintf(os.Stderr, "on MatchString: %v\n", regexperr)
			return err
		}
		if matched {
			// 他のgoroutineが更新中。エラーにしない
		} else {
			return err
		}
	}

	var user user
	err = d.client.Table("Main").Get("PK", userKey).Range("SK", dynamo.Equal, userKey).Consistent(true).One(&user)
	return err
}

func (d AppDynamoDB) SimpleRead() error {
	var user user
	userKey := fmt.Sprintf("User%d", configs.RandUserId())
	err := d.client.Table("Main").Get("PK", userKey).Range("SK", dynamo.Equal, userKey).One(&user)
	return err
}

func (d AppDynamoDB) TransactWrite() error {
	table := d.client.Table("Main")
	userId := configs.RandUserId()
	userKey := fmt.Sprintf("User%d", userId)
	userItemKey := fmt.Sprintf("UserItem%d", userId)
	update1 := table.Update("PK", userKey).Range("SK", userKey).Add("Gold", -5).If("$ >= ?", "Gold", 5)
	update2 := table.Update("PK", userItemKey).Range("SK", userItemKey).Add("NumTickets", 1)

	err := d.client.WriteTx().Update(update1).Update(update2).Run()
	if err != nil {
		matched, regexperr := regexp.MatchString("TransactionConflict", err.Error())
		if regexperr != nil {
			fmt.Fprintf(os.Stderr, "on MatchString: %v\n", regexperr)
			return err
		}
		if matched {
			// 他のgoroutineが更新中。エラーにしない
			return nil
		} else {
			return err
		}
	}
	return err
}
