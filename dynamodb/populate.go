package dynamodb

import (
	"fmt"
)

type user struct {
	PK   string
	SK   string
	Gold int
}

func (d AppDynamoDB) populate(startIndex int, num int) error {
	table := d.client.Table("Main")
	items := make([]interface{}, num)
	for i := 0; i < num; i++ {
		key := fmt.Sprintf("User%d", startIndex+i)
		items[i] = user{
			PK:   key,
			SK:   key,
			Gold: 10000,
		}
	}

	wrote, err := table.Batch().Write().Put(items...).Run()
	if err != nil {
		return err
	}
	if wrote != num {
		return fmt.Errorf("incomplete Put: requested %d, wrote %d", num, wrote)
	}
	return nil
}

func (d AppDynamoDB) PopulateMany(numUsers int) error {
	for i := 1; i <= numUsers; i += 25 {
		var err error
		if i+24 > numUsers {
			err = d.populate(i, numUsers-i+1)
		} else {
			err = d.populate(i, 25)
		}
		if err != nil {
			return err
		}
	}
	return nil
}
