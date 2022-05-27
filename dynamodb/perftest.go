package dynamodb

import (
	"fmt"
	"time"

	"github.com/irohiroki/spanner-performance-test/configs"
)

func (d AppDynamoDB) MeasureTransaction() (time.Duration, error) {
	start := time.Now()
	table := d.client.Table("Main")
	userId := configs.RandUserId()
	userKey := fmt.Sprintf("User%d", userId)
	userItemKey := fmt.Sprintf("UserItem%d", userId)
	update1 := table.Update("PK", userKey).Range("SK", userKey).Add("Gold", -5).If("$ >= ?", "Gold", 5)
	update2 := table.Update("PK", userItemKey).Range("SK", userItemKey).Add("NumTickets", 1)

	err := d.client.WriteTx().Update(update1).Update(update2).Run()
	return time.Since(start), err
}
