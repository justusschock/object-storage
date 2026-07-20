package s3_test

import (
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	s3adapter "github.com/justusschock/go-object-storage/backend/s3"
)

func ExampleNew() {
	client := awss3.NewFromConfig(aws.Config{Region: "us-east-1", Credentials: aws.AnonymousCredentials{}})
	store, err := s3adapter.New(client, manager.NewUploader(client))
	fmt.Println(store != nil, err == nil)
	// Output: true true
}
