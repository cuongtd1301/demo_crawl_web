package infrastructure

import (
	"log"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
)

const (
	region = "us-east-2"
	bucket = "demo-storage-file"
)

var (
	awsSession *session.Session
)

func loadAwsService() {
	var err error
	awsSession, err = session.NewSession(&aws.Config{
		Region: aws.String(region),
		// Credentials: credentials.NewStaticCredentials("AKID", "SECRET_KEY", "TOKEN"),
	},
	)

	if err != nil {
		log.Fatal("Unable to connect sdk: ", err)
	}
}

func GetBucketName() string {
	return bucket
}

func GetAwsSession() *session.Session {
	return awsSession
}
