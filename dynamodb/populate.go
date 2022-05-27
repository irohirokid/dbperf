package dynamodb

import "fmt"

type user struct {
	PK   string
	SK   string
	Gold int
}

func (d AppDynamoDB) PopulateMany(numUsers int) error {
	table := d.client.Table("Main")

	for i := 1; i <= numUsers; i++ {
		key := fmt.Sprintf("User%d", i)
		item := user{
			PK:   key,
			SK:   key,
			Gold: 10000,
		}
		err := table.Put(item).Run()
		if err != nil {
			return err
		}
	}
	return nil
}
