package main

import (
	"fmt"
	"os"

	"github.com/irohiroki/spanner-performance-test/cloudspanner"
	"github.com/irohiroki/spanner-performance-test/configs"
	"github.com/irohiroki/spanner-performance-test/db"
	dynamodb2 "github.com/irohiroki/spanner-performance-test/dynamodb"
)

func main() {
	if len(os.Args) < 4 {
		fmt.Println("Usage: dbperf <service_name> <subcommand> <db_id>")
		return
	}

	var appDb db.Client
	var err error
	switch os.Args[1] {
	case "spanner":
		appDb, err = cloudspanner.NewClient(os.Args[3])
	case "dynamodb":
		appDb, err = dynamodb2.NewClient(os.Args[3])
	}
	if err != nil {
		fmt.Println(err)
		return
	}

	switch os.Args[2] {
	case "populate":
		err = appDb.PopulateMany(configs.NumUsers)
	case "perftest":
		err = perfTest(appDb)
	}
	if err != nil {
		fmt.Println(err)
	}
}
