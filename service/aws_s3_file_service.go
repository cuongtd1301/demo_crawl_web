package service

import (
	"bytes"
	"context"
	"crawlweb/infrastructure"
	"fmt"
	"io"
	"log"
	"os"
	"path"
	"sort"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/s3"
)

const (
	PART_SIZE = 6000000 // 5_000_000 minimal
	RETRIES   = 2
)

func UploadFileToBucket(url string) (s3Filename string, etag string) {
	if url == "" {
		return
	}
	svc := s3.New(infrastructure.GetAwsSession())

	// read File
	fileName := GenCode() + ".jpg"
	filePath := "./temp/" + fileName
	err := DownloadFile(url, filePath)
	if err != nil {
		log.Println("Error:", err.Error())
		return
	}
	tempFile, err := os.Open(filePath)
	if err != nil {
		log.Printf("Read file error: %+v\n", err)
		return
	}
	defer tempFile.Close()
	stats, _ := tempFile.Stat()
	fileSize := stats.Size()
	// upload File
	if fileSize <= PART_SIZE*2 {
		log.Println("uploadNormalFile")
		return uploadNormalFile(svc, tempFile)
	}
	log.Println("uploadLargeFile")
	return uploadLargeFile(svc, tempFile, fileSize)
}

func uploadLargeFile(svc *s3.S3, tempFile *os.File, fileSize int64) (s3Filename string, etag string) {
	// put file in byteArray
	buffer := make([]byte, fileSize)
	tempFile.Read(buffer)

	// Create MultipartUpload object
	ctxTimeout, cancel := context.WithTimeout(context.Background(), time.Second*60)
	defer cancel()
	multiOutput, err := svc.CreateMultipartUploadWithContext(ctxTimeout, &s3.CreateMultipartUploadInput{
		Bucket: aws.String(infrastructure.GetBucketName()),
		Key:    aws.String(tempFile.Name()),
	})
	if err != nil {
		fmt.Println(err)
		return
	}

	// multipart upload
	var start, currentSize int
	var remaining = int(fileSize)
	var partNum = 1
	completedPartChannel := make(chan *s3.CompletedPart)
	defer close(completedPartChannel)
	for start = 0; remaining != 0; start += PART_SIZE {
		if remaining < PART_SIZE {
			currentSize = remaining
		} else {
			currentSize = PART_SIZE
		}
		go uploadPartFile(svc, multiOutput, buffer[start:start+currentSize], partNum, completedPartChannel)
		// Detract the current part size from remaining
		remaining -= currentSize

		partNum++
	}

	// append completedPart
	listcompletedParts := []*s3.CompletedPart{}
	for i := 0; i < partNum-1; i++ {
		tmp := <-completedPartChannel
		if tmp == nil {
			// Abort Upload if any parts get error
			ctxTimeout2, cancel2 := context.WithTimeout(context.Background(), time.Second*10)
			defer cancel2()
			svc.AbortMultipartUploadWithContext(ctxTimeout2, &s3.AbortMultipartUploadInput{
				Bucket: aws.String(infrastructure.GetBucketName()),
				Key:    aws.String(tempFile.Name()),
			})
			log.Printf("About upload because some parts get error\n")
			return
		}
		listcompletedParts = append(listcompletedParts, tmp)
	}
	sort.Slice(listcompletedParts, func(i, j int) bool {
		return int(*listcompletedParts[i].PartNumber) < int(*listcompletedParts[j].PartNumber)
	})

	// complete upload
	resp, err := svc.CompleteMultipartUpload(&s3.CompleteMultipartUploadInput{
		Bucket:   multiOutput.Bucket,
		Key:      multiOutput.Key,
		UploadId: multiOutput.UploadId,
		MultipartUpload: &s3.CompletedMultipartUpload{
			Parts: listcompletedParts,
		},
	})
	if err != nil {
		log.Println(err)
		return
	}
	return tempFile.Name(), *resp.ETag
}

// Uploads the fileBytes bytearray a MultiPart upload
func uploadPartFile(svc *s3.S3, multiOutput *s3.CreateMultipartUploadOutput, fileBytes []byte, partNum int, completedParts chan *s3.CompletedPart) {
	ctxTimeout, cancel := context.WithTimeout(context.Background(), time.Second*60)
	defer cancel()
	var try int
	for try <= RETRIES {
		uploadResp, err := svc.UploadPartWithContext(ctxTimeout, &s3.UploadPartInput{
			Body:          bytes.NewReader(fileBytes),
			Bucket:        multiOutput.Bucket,
			Key:           multiOutput.Key,
			PartNumber:    aws.Int64(int64(partNum)),
			UploadId:      multiOutput.UploadId,
			ContentLength: aws.Int64(int64(len(fileBytes))),
		})
		// Upload failed
		if err != nil {
			fmt.Println(err)
			// Max retries reached! Quitting
			if try == RETRIES {
				completedParts <- nil
				log.Println("partNum retries fail:", partNum)
				return
			} else {
				// Retrying
				try++
			}
		} else {
			// Upload is done!
			completedParts <- &s3.CompletedPart{
				ETag:       uploadResp.ETag,
				PartNumber: aws.Int64(int64(partNum)),
			}
			fmt.Printf("Part %v complete\n", partNum)
			return
		}
	}
	return
}

func uploadNormalFile(svc *s3.S3, tempFile *os.File) (s3Filename string, etag string) {
	ctxTimeout, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()
	result, err := svc.PutObjectWithContext(ctxTimeout, &s3.PutObjectInput{
		Body:   tempFile,
		Bucket: aws.String(infrastructure.GetBucketName()),
		Key:    aws.String(tempFile.Name()),
	})
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
	s3Filename = tempFile.Name()
	etag = *result.ETag
	return
}

func DownloadFileFromBucket(filename string, localFilePath string) error {
	svc := s3.New(infrastructure.GetAwsSession())
	input := &s3.GetObjectInput{
		Bucket: aws.String("demo-storage-file"),
		Key:    aws.String(filename),
	}

	ctxTimeout, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()
	result, err := svc.GetObjectWithContext(ctxTimeout, input)
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
