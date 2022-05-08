package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"cloud.google.com/go/spanner"
)

type AppSpanner struct {
	client *spanner.Client
}

type AppDbClient interface {
	MeasureTransaction() (time.Duration, error)
	PopulateMany(numUsers int) error
}

func NewAppSpannerClient(db_id string) (AppDbClient, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
	defer cancel()

	client, err := spanner.NewClient(ctx, db_id)
	if err != nil {
		return nil, err
	}
	return &AppSpanner{client}, nil
}

func main() {
	if len(os.Args) < 4 {
		fmt.Println("Usage: dbperf <service_name> <subcommand> <db_id>")
		return
	}

	var appDb AppDbClient
	var err error
	switch os.Args[1] {
	case "spanner":
		appDb, err = NewAppSpannerClient(os.Args[3])
	}
	if err != nil {
		fmt.Println(err)
		return
	}

	switch os.Args[2] {
	case "populate":
		err = appDb.PopulateMany(100000)
	case "perftest":
		err = perfTest(appDb)
	}
	if err != nil {
		fmt.Println(err)
	}
}
