package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/golang/protobuf/jsonpb"
	"github.com/sberss/cas3/internal/proto"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
)

func main() {
	rootCommand := &cobra.Command{
		Use:   "cas3-client",
		Short: "A client for interacting with the CAS3 storage server",
	}

	getObjectCommand := &cobra.Command{
		Use:   "get-object",
		Short: "Get the specified object from CAS3 and send to STDOUT.",
		Run: func(cmd *cobra.Command, args []string) {
			host, _ := cmd.Flags().GetString("host")
			port, _ := cmd.Flags().GetString("port")
			etag, _ := cmd.Flags().GetString("etag")
			err := getObject(host, port, etag)
			if err != nil {
				log.Fatal(err)
			}
		},
	}
	getObjectCommand.PersistentFlags().String("host", "localhost", "Server host.")
	getObjectCommand.PersistentFlags().String("port", "8082", "Server port.")
	getObjectCommand.PersistentFlags().String("etag", "", "Etag of object to retrieve.")
	rootCommand.AddCommand(getObjectCommand)

	putObjectCommand := &cobra.Command{
		Use:   "put-object",
		Short: "Upload the given object to CAS3",
		Run: func(cmd *cobra.Command, args []string) {
			host, _ := cmd.Flags().GetString("host")
			port, _ := cmd.Flags().GetString("port")
			objectPath, _ := cmd.Flags().GetString("object-path")
			err := putObject(host, port, objectPath)
			if err != nil {
				log.Fatal(err)
			}
		},
	}
	putObjectCommand.PersistentFlags().String("host", "localhost", "Server host.")
	putObjectCommand.PersistentFlags().String("port", "8082", "Server port.")
	putObjectCommand.PersistentFlags().String("object-path", "", "Path of object to write.")
	rootCommand.AddCommand(putObjectCommand)

	if err := rootCommand.Execute(); err != nil {
		log.Fatal(err)
	}
}

func buildClient(host, port string) (proto.StoreClient, error) {
	conn, err := grpc.Dial(fmt.Sprintf("%s:%s", host, port), grpc.WithInsecure())
	if err != nil {
		return nil, err
	}
	return proto.NewStoreClient(conn), nil
}

func getObject(host, port, etag string) error {
	client, err := buildClient(host, port)
	if err != nil {
		return err
	}

	getObjectRequest := &proto.GetObjectRequest{
		Etag: etag,
	}

	getObjectResponse, err := client.GetObject(context.Background(), getObjectRequest)
	if err != nil {
		return err
	}

	os.Stdout.Write(getObjectResponse.Object)

	return nil
}

func putObject(host, port, objectPath string) error {
	client, err := buildClient(host, port)
	if err != nil {
		return err
	}

	object, err := os.ReadFile(objectPath)
	if err != nil {
		return err
	}

	putObjectRequest := &proto.PutObjectRequest{
		Object: object,
	}

	putObjectResponse, err := client.PutObject(context.Background(), putObjectRequest)
	if err != nil {
		return err
	}

	m := jsonpb.Marshaler{}
	result, _ := m.MarshalToString(putObjectResponse)
	fmt.Printf("%s\n", result)

	return nil
}
