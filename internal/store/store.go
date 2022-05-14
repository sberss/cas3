package store

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type Config struct {
	AwsConfig      aws.Config
	BackingBucket  string
	Concurrency    int
	ChunkSizeBytes int
}

type Store interface {
	GetChunk(string) ([]byte, error)
	StartGet(string) []string
	PutChunk([]byte) (string, error)
	FinishPut([]string) string
}

type s3Store struct {
	config     *Config
	client     *s3.Client
	downloader *manager.Downloader
	uploader   *manager.Uploader

	chunkStore map[string][]string
}

func NewS3Store(config *Config) *s3Store {
	client := s3.NewFromConfig(config.AwsConfig)
	downloader := manager.NewDownloader(client, func(d *manager.Downloader) { d.Concurrency = config.Concurrency })
	uploader := manager.NewUploader(client, func(u *manager.Uploader) { u.Concurrency = config.Concurrency })

	return &s3Store{
		config:     config,
		client:     client,
		downloader: downloader,
		uploader:   uploader,

		chunkStore: make(map[string][]string),
	}
}

// GetChunk takes the etag of a chunk and gets it from the S3 backing store.
func (s *s3Store) GetChunk(etag string) ([]byte, error) {
	getObjectInput := &s3.GetObjectInput{
		Bucket: aws.String(s.config.BackingBucket),
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

// StartGet takes the etag representing a full object and returns a list of etags representing each chunk in order.
func (s *s3Store) StartGet(etag string) []string {
	return s.chunkStore[etag]
}

// PutChunk takes a byte array representing a chunk of an object and stores it in the S3 store,
// using the md5 sum as the key.
func (s *s3Store) PutChunk(object []byte) (string, error) {
	if len(object) > s.config.ChunkSizeBytes {
		fmt.Println(len(object))
		return "", &ExceededChunkSizeError{s.config.ChunkSizeBytes, len(object)}
	}

	md5 := md5.Sum(object)
	encodedMd5 := base64.StdEncoding.EncodeToString(md5[:])
	etag := hex.EncodeToString(md5[:])

	// If object already exists in backing store, do not upload again.
	if s.getObjectBytes(etag) > 0 {
		return etag, nil
	}

	putObjectInput := &s3.PutObjectInput{
		Body:          bytes.NewReader(object),
		Bucket:        aws.String(s.config.BackingBucket),
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

// FinishPut takes a list of etags and calculates an etag representing this list and persists it. The calculated etag
// is then returned.
func (s *s3Store) FinishPut(etags []string) string {
	md5 := md5.Sum([]byte(strings.Join(etags, "\n")))
	etag := hex.EncodeToString(md5[:])

	s.chunkStore[etag] = etags

	return etag
}

// getObjectBytes attempts to HEAD the S3 object stored at the given etag. If found it returns the size of the object.
// On any error, a size of 0 is returned.
func (s *s3Store) getObjectBytes(etag string) int64 {
	headObjectInput := &s3.HeadObjectInput{
		Bucket: aws.String(s.config.BackingBucket),
		Key:    aws.String(etag),
	}

	objectMetadata, err := s.client.HeadObject(context.Background(), headObjectInput)
	if err != nil {
		return 0
	}

	return objectMetadata.ContentLength
}

type ExceededChunkSizeError struct {
	maxChunkSize    int
	actualChunkSize int
}

func (e *ExceededChunkSizeError) Error() string {
	return fmt.Sprintf("Chunk size %dMB exceeds configured max chunk size %dMB)", e.actualChunkSize, e.maxChunkSize)
}
