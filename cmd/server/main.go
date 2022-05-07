package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/sberss/cas3/internal/server"
	"github.com/sberss/cas3/internal/store"
)

type appContext struct {
	storeConfig *store.Config
}

func main() {
	appCtx, err := buildContext()
	if err != nil {
		log.Fatal(err)
	}

	srv, err := server.NewServer(appCtx.storeConfig)
	if err != nil {
		log.Fatal(err)
	}
	// Setup signal handling.
	shutdownChan := make(chan os.Signal, 1)
	signal.Notify(shutdownChan, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)

	// Start server.
	srv.Start()

	// Block until we are terminated.
	<-shutdownChan

	// Try and shutdown our listeners gracefully.
	srv.Stop()
}

func buildContext() (*appContext, error) {
	storeType := os.Getenv("STORE_TYPE")
	if storeType == "" {
		storeType = "local"
	}

	awsS3Endpoint := os.Getenv("AWS_S3_ENDPOINT")

	awsS3EndpointResolver := aws.EndpointResolverWithOptionsFunc(func(service, region string, options ...interface{}) (aws.Endpoint, error) {
		if service == s3.ServiceID && awsS3Endpoint != "" {
			return aws.Endpoint{
				URL:               awsS3Endpoint,
				SigningRegion:     "us-east-1",
				HostnameImmutable: true,
			}, nil
		}

		// Fallback to default resolving
		return aws.Endpoint{}, &aws.EndpointNotFoundError{}
	})

	awsConfig, err := config.LoadDefaultConfig(context.Background(), config.WithEndpointResolverWithOptions(awsS3EndpointResolver))
	if err != nil {
		return nil, err
	}

	storeConfig := &store.Config{
		StoreType:       storeType,
		StoreTypeConfig: awsConfig,
	}

	return &appContext{
		storeConfig: storeConfig,
	}, nil
}
