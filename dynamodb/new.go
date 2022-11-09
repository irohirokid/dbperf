package dynamodb

import (
	"context"
	"errors"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/guregu/dynamo"
	"github.com/irohirokid/dbperf/db"
)

type AppDynamoDB struct {
	client *dynamo.DB
}

func NewClient(endpoint string) (db.Client, error) {
	cfg, err := config.LoadDefaultConfig(
		context.Background(),
		config.WithRegion("ap-northeast-1"),
		config.WithEndpointResolverWithOptions(
			aws.EndpointResolverWithOptionsFunc(
				func(service, region string, options ...interface{}) (aws.Endpoint, error) {
					return aws.Endpoint{URL: "http://localhost:8000"}, nil
				},
			),
		),
	)
	if err != nil {
		return nil, err
	}
	client := dynamo.New(cfg)
	// client := dynamo.New(session.Must(session.NewSession()), &aws.Config{
	// Region:   aws.String(os.Getenv("AWS_REGION")),
	// Endpoint: aws.String(endpoint),
	// })
	if client == nil {
		return nil, errors.New("cloudn't create DynamoDB client")
	}

	return &AppDynamoDB{client}, nil
}
