package service

import (
	"bytes"
	"context"
	"crawlweb/infrastructure"
	"crawlweb/model"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path"
	"path/filepath"
	"sort"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/s3"
)

const (
	PART_SIZE       = 5_000_000 // 5_000_000 minimal
	RETRIES         = 2
	LARGE_FILE_SIZE = 20_000_000
)

func UploadFileToBucket(url string, mimeType string) (s3Filename string, etag string, err error) {
	if url == "" {
		return
	}
	svc := s3.New(infrastructure.GetAwsSession())

	// read File
	fileName := GenCode() + ".jpg"
	filePath := "./temp/" + fileName
	err = DownloadFile(url, filePath)
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
	if fileSize <= LARGE_FILE_SIZE {
		log.Println("uploadNormalFile")
		s3Filename, etag = uploadNormalFile(svc, tempFile)
	}
	log.Println("uploadLargeFile")
	s3Filename, etag = uploadLargeFile(svc, tempFile, fileSize)

	err = Insert(model.FileUploadInfo{
		FileSize: fileSize,
		FileName: fileName,
		Ext:      filepath.Ext(fileName),
		MimeType: mimeType,
	})
	if err != nil {
		log.Println("insert file to db fail:", err)
	}

	return
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
	var try int
	for try <= RETRIES {
		ctxTimeout, cancel := context.WithTimeout(context.Background(), time.Second*60)
		defer cancel()
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

	ctxTimeoutHeader, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()
	resultHeader, err := svc.HeadObjectWithContext(ctxTimeoutHeader, &s3.HeadObjectInput{
		Bucket: aws.String("demo-storage-file"),
		Key:    aws.String(filename),
	})
	if err != nil {
		log.Println(err)
		return err
	}

	localFile := path.Join(localFilePath, filename)
	data := []byte{}
	if int(*resultHeader.ContentLength) <= LARGE_FILE_SIZE {
		err := downloadNormalFile(svc, filename, data)
		if err != nil {
			return err
		}
		err = ioutil.WriteFile(localFile, data, 0644)
		if err != nil {
			return err
		}
	} else {
		err := downloadLargeFile(svc, filename, int(*resultHeader.ContentLength), data)
		if err != nil {
			return err
		}
		err = ioutil.WriteFile(localFile, data, 0644)
		if err != nil {
			return err
		}
	}

	return nil
}

func downloadNormalFile(svc *s3.S3, filename string, data []byte) error {
	ctxTimeout, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()
	result, err := svc.GetObjectWithContext(ctxTimeout, &s3.GetObjectInput{
		Bucket: aws.String("demo-storage-file"),
		Key:    aws.String(filename),
	})
	if err != nil {
		log.Println(err)
		return err
	}
	data, err = ioutil.ReadAll(result.Body)
	if err != nil {
		log.Println(err)
		return err
	}
	return nil
}

func downloadLargeFile(svc *s3.S3, filename string, contentLength int, data []byte) error {
	for startRange := 0; startRange < contentLength; startRange += PART_SIZE {

		var try int
		for try <= RETRIES {
			ctxTimeout, cancel := context.WithTimeout(context.Background(), time.Second*20)
			defer cancel()
			result, err := svc.GetObjectWithContext(ctxTimeout, &s3.GetObjectInput{
				Bucket: aws.String(infrastructure.GetBucketName()),
				Key:    aws.String(filename),
				Range:  aws.String(fmt.Sprintf("bytes=%d-%d", startRange, startRange+PART_SIZE-1)),
			})
			if err != nil {
				fmt.Println(err)
				// Max retries reached! Quitting
				if try == RETRIES {
					return err
				} else {
					// Retrying
					try++
					continue
				}
			}
			tmpData, _ := ioutil.ReadAll(result.Body)
			data = append(data, tmpData...)
			break
		}
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
