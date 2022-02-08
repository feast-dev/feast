package main

import (
	"context"
	"github.com/feast-dev/feast/go/protos/feast/serving"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
	"google.golang.org/grpc/test/bufconn"
	"net"
	"testing"
)

func getClient(ctx context.Context) (serving.ServingServiceClient, func()) {
	buffer := 1024 * 1024
	listener := bufconn.Listen(buffer)

	server := grpc.NewServer()
	serving.RegisterServingServiceServer(server, &servingServiceServer{})
	go func() {
		if err := server.Serve(listener); err != nil {
			panic(err)
		}
	}()

	conn, _ := grpc.DialContext(ctx, "", grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
		return listener.Dial()
	}), grpc.WithInsecure())

	closer := func() {
		listener.Close()
		server.Stop()
	}

	client := serving.NewServingServiceClient(conn)

	return client, closer
}

func TestGetFeastServingInfo(t *testing.T) {
	ctx := context.Background()
	client, closer := getClient(ctx)
	defer closer()
	response, err := client.GetFeastServingInfo(ctx, &serving.GetFeastServingInfoRequest{})
	assert.Nil(t, err)
	assert.Equal(t, feastServerVersion, response.Version)
}

func TestGetOnlineFeatures(t *testing.T) {
	ctx := context.Background()
	client, closer := getClient(ctx)
	defer closer()
	response, err := client.GetOnlineFeatures(ctx, &serving.GetOnlineFeaturesRequest{})
	// TODO: change this once everything works
	assert.Nil(t, response)
	assert.NotNil(t, err)
}
