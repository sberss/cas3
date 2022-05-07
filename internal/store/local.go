package store

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"os"
)

// localStore is a local store implementation used for testing.
type localStore struct {
	path string
}

func newLocalStore(path string) (*localStore, error) {
	err := os.MkdirAll(path, os.ModePerm)
	if err != nil {
		return nil, err
	}

	return &localStore{
		path: path,
	}, nil
}

// Get takes an etag and gets the corresponding object from the local store.
func (l *localStore) Get(etag string) ([]byte, error) {
	return os.ReadFile(fmt.Sprintf("%s/%s", l.path, etag))
}

// Put takes a byte array representing an object and stores it in the local store.
func (l *localStore) Put(object []byte) (string, error) {
	md5 := md5.Sum(object)
	etag := hex.EncodeToString(md5[:])

	writePath := fmt.Sprintf("%s/%s", l.path, etag)
	if _, err := os.Stat(writePath); err == nil {
		// Object already exists, no need to rewrite.
		return etag, nil
	}

	err := os.WriteFile(writePath, object, 0600)
	if err != nil {
		return "", err
	}
	return etag, nil
}
