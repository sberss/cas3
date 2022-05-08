package store

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/base64"
	"encoding/hex"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type s3Store struct {
	backingBucket string
	client        *s3.Client
	downloader    *manager.Downloader
	uploader      *manager.Uploader
}

func newS3Store(backingBucket string, concurrency int, config aws.Config) *s3Store {
	client := s3.NewFromConfig(config)
	downloader := manager.NewDownloader(client, func(d *manager.Downloader) { d.Concurrency = concurrency })
	uploader := manager.NewUploader(client, func(u *manager.Uploader) { u.Concurrency = concurrency })

	return &s3Store{
		backingBucket: backingBucket,
		client:        client,
		downloader:    downloader,
		uploader:      uploader,
	}
}

// Get takes the etag of an object and gets it from the S3 backing store.
func (s *s3Store) Get(etag string) ([]byte, error) {
	getObjectInput := &s3.GetObjectInput{
		Bucket: aws.String(s.backingBucket),
		Key:    aws.String(etag),
	}

	object := make([]byte, s.getObjectBytes(etag))
	buf := manager.NewWriteAtBuffer(object)
	_, err := s.downloader.Download(context.Background(), buf, getObjectInput)
	if err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

// Put takes a byte array and stores it in the S3 store, using the md5 sum as the key.
func (s *s3Store) Put(object []byte) (string, error) {
	md5 := md5.Sum(object)
	encodedMd5 := base64.StdEncoding.EncodeToString(md5[:])
	etag := hex.EncodeToString(md5[:])

	// If object already exists in backing store, do not upload again.
	if s.getObjectBytes(etag) > 0 {
		return etag, nil
	}

	putObjectInput := &s3.PutObjectInput{
		Body:          bytes.NewReader(object),
		Bucket:        aws.String(s.backingBucket),
		Key:           aws.String(etag),
		ContentLength: int64(len(object)),
		ContentMD5:    aws.String(encodedMd5),
	}
	_, err := s.uploader.Upload(context.Background(), putObjectInput)
	if err != nil {
		return "", err
	}

	return etag, nil
}

// getObjectBytes attempts to HEAD the S3 object stored at the given etag. If found it returns the size of the object.
// On any error, a size of 0 is returned.
func (s *s3Store) getObjectBytes(etag string) int64 {
	headObjectInput := &s3.HeadObjectInput{
		Bucket: aws.String(s.backingBucket),
		Key:    aws.String(etag),
	}

	objectMetadata, err := s.client.HeadObject(context.Background(), headObjectInput)
	if err != nil {
		return 0
	}

	return objectMetadata.ContentLength
}
