package main

import (
	"bytes"
	"fmt"
	"log"
	"math/rand"
	"sync"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

const (
	bucket = "grail-joshnewman"
	prefix = "tmp/s3uploaderror"
	nFile  = 1 // number of files to write to.
)

func upload(client *s3.S3, id int) {
	r := rand.New(rand.NewSource(int64(id)))
	key := aws.String(fmt.Sprintf("%s/test%d", prefix, r.Intn(nFile)))

	var uploadID string
	{
		params := &s3.CreateMultipartUploadInput{
			Bucket: aws.String(bucket),
			Key:    key,
		}
		resp, err := client.CreateMultipartUpload(params)
		if err != nil {
			panic(err)
		}
		uploadID = *resp.UploadId
	}
	log.Printf("start writing to %s (%s)", *key, uploadID)
	var completedParts []*s3.CompletedPart
	{
		data := bytes.NewReader([]byte("hello"))
		partNum := int64(1)
		params := &s3.UploadPartInput{
			Bucket:     aws.String(bucket),
			Key:        key,
			Body:       data,
			UploadId:   aws.String(uploadID),
			PartNumber: aws.Int64(partNum),
		}
		resp, err := client.UploadPart(params)
		if err != nil {
			panic(err)
		}
		completedParts = append(completedParts, &s3.CompletedPart{ETag: resp.ETag, PartNumber: &partNum})
	}
	{
		params := &s3.CompleteMultipartUploadInput{
			Bucket:          aws.String(bucket),
			Key:             key,
			UploadId:        aws.String(uploadID),
			MultipartUpload: &s3.CompletedMultipartUpload{Parts: completedParts},
		}
		_, err := client.CompleteMultipartUpload(params)
		if err != nil {
			log.Panicf("close error: %s %v", *key, err)
		}
	}
	log.Printf("done writing to %s (%s)", *key, uploadID)
}

func main() {
	var opts session.Options
	opts.Config.Region = aws.String("us-west-2")
	s, err := session.NewSessionWithOptions(opts)
	if err != nil {
		panic(err)
	}
	client := s3.New(s)

	wg := sync.WaitGroup{}
	for i := 0; i < 4000; i++ {
		wg.Add(1)
		i := i
		go func() {
			upload(client, i)
			wg.Done()
		}()
	}
	wg.Wait()
}
