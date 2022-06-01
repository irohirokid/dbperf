package dynamodb

import (
	"fmt"

	"github.com/guregu/dynamo"
	"github.com/irohirokid/dbperf/configs"
)

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
	return err
}
