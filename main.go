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

func NewAppSpannerClient() (*AppSpanner, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
	defer cancel()

	client, err := spanner.NewClient(ctx, "projects/your-project-id/instances/test-instance/databases/main")
	if err != nil {
		return nil, err
	}
	return &AppSpanner{client}, nil
}

func main() {
	if len(os.Args) < 2 {
		fmt.Println("Please specify subcommand.")
		return
	}

	appClient, err := NewAppSpannerClient()
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
