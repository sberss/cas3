package store

import (
	"errors"

	"github.com/aws/aws-sdk-go-v2/aws"
)

type Config struct {
	StoreType       string
	StoreTypeConfig interface{}
}

type Store interface {
	Get(string) ([]byte, error)
	Put([]byte) (string, error)
}

func NewStore(config *Config) (Store, error) {
	switch config.StoreType {
	case "local":
		return newLocalStore("/tmp/keep")
	case "s3":
		return newS3Store("cas", 8, config.StoreTypeConfig.(aws.Config)), nil
	}
	return nil, errors.New("unrecognised store type")
}
