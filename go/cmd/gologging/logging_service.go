package logging

import (
	"context"

	"github.com/feast-dev/feast/go/protos/feast/serving"
)

type loggingServiceServer struct {
	channel *chan string
	serving.UnimplementedServingServiceServer
}

func newLoggingServiceServer(channel *chan string) *loggingServiceServer {
	return &loggingServiceServer{channel: channel}
}

func (s *loggingServiceServer) sendLogData(ctx context.Context, log *serving.Log) (*serving.LoggingResponse, error) {
	// Actually use the channel and update the channel with the new log.
	// return the status
}
