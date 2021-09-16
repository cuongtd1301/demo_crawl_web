package service

import (
	"context"
	"crawlweb/infrastructure"
	"errors"
	"fmt"
	"log"
	"sort"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
)

func GetPresignedUrlUploadFile(bucketname, filename string) string {
	if filename == "" {
		return ""
	}
	svc := s3.New(infrastructure.GetAwsSession())
	res, _ := svc.PutObjectRequest(&s3.PutObjectInput{
		Bucket: aws.String(bucketname),
		Key:    aws.String(filename),
	})

	// Create the pre-signed url with an expiry
	url, err := res.Presign(5 * time.Minute)
	if err != nil {
		fmt.Println("Failed to generate a pre-signed url: ", err)
		return ""
	}
	return url
}

func GetPresignedUrlUploadLargeFile(bucketname, filename string, sizeFile int64, partSize int) (uploadId string, presignedUrlPart []PresignedUrlPart, err error) {
	if filename == "" {
		return "", nil, errors.New("filename is EMPTY")
	}
	svc := s3.New(infrastructure.GetAwsSession())
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()
	out, err := svc.CreateMultipartUploadWithContext(ctx, &s3.CreateMultipartUploadInput{
		Bucket: aws.String(bucketname),
		Key:    aws.String(filename),
	})
	if err != nil {
		fmt.Println("Failed to CreateMultipartUpload:", err)
		return "", nil, err
	}
	var start, currentSize int
	var remaining = int(sizeFile)
	var partNum = 1
	for start = 0; remaining != 0; start += partSize {
		if remaining < partSize*2 {
			currentSize = remaining
		} else {
			currentSize = partSize
		}

		r1, _ := svc.UploadPartRequest(&s3.UploadPartInput{
			Bucket:        out.Bucket,
			Key:           out.Key,
			PartNumber:    aws.Int64(int64(partNum)),
			UploadId:      out.UploadId,
			ContentLength: aws.Int64(int64(currentSize)),
		})

		url, err := r1.Presign(15 * time.Minute)
		if err != nil {
			fmt.Println("Failed to generate a pre-signed url of part ", partNum, ":", err)
			return "", nil, errors.New(fmt.Sprintln("Failed to generate a pre-signed url of part ", partNum, ":", err))
		}
		presignedUrlPart = append(presignedUrlPart, PresignedUrlPart{
			Key:        *out.Key,
			PartNumber: partNum,
			Url:        url,
		})
		// Detract the current part size from remaining
		remaining -= currentSize

		partNum++
	}
	uploadId = *out.UploadId
	return
}

func CompleteMultipartUpload(bucketname, filename, uploadId string, listcompletedParts []*s3.CompletedPart) (string, error) {
	sort.Slice(listcompletedParts, func(i, j int) bool {
		return int(*listcompletedParts[i].PartNumber) < int(*listcompletedParts[j].PartNumber)
	})
	svc := s3.New(infrastructure.GetAwsSession())
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	out, err := svc.CompleteMultipartUploadWithContext(ctx, &s3.CompleteMultipartUploadInput{
		Bucket:   aws.String(bucketname),
		Key:      aws.String(filename),
		UploadId: aws.String(uploadId),
		MultipartUpload: &s3.CompletedMultipartUpload{
			Parts: listcompletedParts,
		},
	})
	if err != nil {
		log.Println(err)
		return "", err
	}
	return *out.ETag, nil
}

func AbortMultipartUpload(bucketname, filename, uploadId string) error {
	svc := s3.New(infrastructure.GetAwsSession())
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	_, err := svc.AbortMultipartUploadWithContext(ctx, &s3.AbortMultipartUploadInput{
		Bucket:   aws.String(bucketname),
		Key:      aws.String(filename),
		UploadId: aws.String(uploadId),
	})
	if err != nil {
		log.Println(err)
		return err
	}
	return nil
}
