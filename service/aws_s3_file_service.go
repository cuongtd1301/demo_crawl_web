package service

import (
	"crawlweb/infrastructure"
	"log"
	"os"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

func UploadFile(url string) (location string) {
	if url == "" {
		return
	}
	fileName := GenCode() + ".jpg"
	filePath := "./temp/" + fileName
	err := DownloadFile(url, filePath)
	if err != nil {
		log.Println("Error:", err.Error())
		return
	}
	tempFile, err := os.Open("./" + filePath)
	if err != nil {
		log.Printf("Read file error: %+v\n", err)
		return
	}
	defer tempFile.Close()
	uploader := s3manager.NewUploader(infrastructure.GetAwsSession())
	tmp, err := uploader.Upload(&s3manager.UploadInput{
		Bucket: aws.String(infrastructure.GetBucketName()),
		Key:    aws.String(fileName),
		Body:   tempFile,
	})
	if err != nil {
		log.Println("error when create file to aws-s3:", err)
		return
	}
	location = tmp.Location
	return
}
