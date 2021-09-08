package service

import (
	"crawlweb/infrastructure"
	"fmt"
	"io"
	"log"
	"os"
	"path"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/s3"
)

func UploadFileToBucket(url string) (s3Filename string, etag string) {
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

	svc := s3.New(infrastructure.GetAwsSession())
	input := &s3.PutObjectInput{
		Body:   tempFile,
		Bucket: aws.String("demo-storage-file"),
		Key:    aws.String(fileName),
	}

	result, err := svc.PutObject(input)
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			default:
				fmt.Println(aerr.Error())
			}
		} else {
			fmt.Println(err.Error())
		}
		return
	}
	s3Filename = fileName
	etag = *result.ETag
	return
}

func DownloadFileFromBucket(filename string, localFilePath string) error {
	svc := s3.New(infrastructure.GetAwsSession())
	input := &s3.GetObjectInput{
		Bucket: aws.String("demo-storage-file"),
		Key:    aws.String(filename),
	}

	result, err := svc.GetObject(input)
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			case s3.ErrCodeNoSuchKey:
				fmt.Println(s3.ErrCodeNoSuchKey, aerr.Error())
			case s3.ErrCodeInvalidObjectState:
				fmt.Println(s3.ErrCodeInvalidObjectState, aerr.Error())
			default:
				fmt.Println(aerr.Error())
			}
		} else {

			fmt.Println(err.Error())
		}
		return err
	}

	localFile := path.Join(localFilePath, filename)
	file, err := os.Create(localFile)
	if err != nil {
		log.Println(err)
		return err
	}
	defer file.Close()

	//Write the bytes to the file
	_, err = io.Copy(file, result.Body)
	if err != nil {
		log.Println(err)
		return err
	}
	return nil
}

/*
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
*/
