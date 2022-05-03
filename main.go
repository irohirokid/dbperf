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

func NewAppSpannerClient(db_id string) (*AppSpanner, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
	defer cancel()

	client, err := spanner.NewClient(ctx, db_id)
	if err != nil {
		return nil, err
	}
	return &AppSpanner{client}, nil
}

func main() {
	if len(os.Args) < 3 {
		fmt.Println("Usage: spannerperf <subcommand> <db_id>")
		return
	}

	appClient, err := NewAppSpannerClient(os.Args[2])
	if err != nil {
		fmt.Println(err)
		return
	}

	if os.Args[1] == "populate" {
		err = appClient.populateMany(100000)
		if err != nil {
			fmt.Println(err)
		}
		return
	}
}
