package main

import (
	"fmt"
	"os"

	"github.com/irohirokid/dbperf/cloudspanner"
	"github.com/irohirokid/dbperf/configs"
	"github.com/irohirokid/dbperf/db"
	dynamodb2 "github.com/irohirokid/dbperf/dynamodb"
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
