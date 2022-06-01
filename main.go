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
	spannerId        = kingpin.Flag("spannerid", "DB identifier").Short('I').String()
	dynamodbEndpoint = kingpin.Flag("endpoint", "Endpoint").Short('e').String()
	operation        = kingpin.Flag("operation", "Operation to perform on DB. One of c(Create), r(Read), u(Update), cr(ConsistentRead), or tw(TransactWrite)").Short('o').Default("r").String()
	duration         = kingpin.Flag("duration", "Performance test duration in second").Short('d').Default("10").Int()
	interval         = kingpin.Flag("interval", "Stat interval in performance test").Short('i').Default("1").Int()
	numLoaders       = kingpin.Flag("loader", "Number of loader threads").Short('l').Default("3").Int()
	reqPerSec        = kingpin.Flag("rps", "Requests per second").Short('r').Default("100").Int()
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
