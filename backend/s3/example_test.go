package s3_test

import (
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	s3adapter "github.com/justusschock/object-storage/backend/s3"
)

func ExampleNewWithOptions() {
	client := awss3.NewFromConfig(aws.Config{Region: "us-east-1", Credentials: aws.AnonymousCredentials{}})
	store, err := s3adapter.NewWithOptions(client, s3adapter.Options{PartSize: 64 << 20, Concurrency: 8})
	fmt.Println(store != nil, err == nil)
	// Output: true true
}
