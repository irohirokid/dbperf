package main

import (
	"fmt"

	"github.com/irohirokid/dbperf/cloudspanner"
	"github.com/irohirokid/dbperf/configs"
	"github.com/irohirokid/dbperf/db"
	dynamodb2 "github.com/irohirokid/dbperf/dynamodb"
	"gopkg.in/alecthomas/kingpin.v2"
)

var (
	service          = kingpin.Flag("service", "`spanner` or `dynamodb`").Short('s').Required().String()
	spannerId        = kingpin.Flag("spannerid", "DB identifier").Short('i').String()
	dynamodbEndpoint = kingpin.Flag("endpoint", "Endpoint").Short('e').String()
	duration         = kingpin.Flag("duration", "Performance test duration in second").Short('d').Default("10").Int()
)

func main() {
	kingpin.Command("populate", "Populate DB.")
	kingpin.Command("perftest", "Run performance test.")
	command := kingpin.Parse()

	var appDb db.Client
	var err error
	switch *service {
	case "spanner":
		appDb, err = cloudspanner.NewClient(*spannerId)
	case "dynamodb":
		appDb, err = dynamodb2.NewClient(*dynamodbEndpoint)
	}
	if err != nil {
		fmt.Println(err)
		return
	}

	switch command {
	case "populate":
		err = appDb.PopulateMany(configs.NumUsers)
	case "perftest":
		err = perfTest(appDb)
	}
	if err != nil {
		fmt.Println(err)
	}
}
