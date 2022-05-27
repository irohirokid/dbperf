package dynamodb

import (
	"errors"
	"os"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/guregu/dynamo"
	"github.com/irohiroki/spanner-performance-test/db"
)

type AppDynamoDB struct {
	client *dynamo.DB
}

func NewClient(endpoint string) (db.Client, error) {
	client := dynamo.New(session.Must(session.NewSession()), &aws.Config{
		Region:   aws.String(os.Getenv("AWS_REGION")),
		Endpoint: aws.String(endpoint),
	})
	if client == nil {
		return nil, errors.New("cloudn't create DynamoDB client")
	}

	return &AppDynamoDB{client}, nil
}
