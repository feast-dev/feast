package main

import (
	"fmt"
	"log"
	"time"

	"github.com/feast-dev/feast/go/internal/feast"
	"github.com/feast-dev/feast/go/protos/feast/serving"
	"github.com/feast-dev/feast/go/protos/feast/types"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type Log struct {
	// Example: driver_id, customer_id
	entityNames []string
	// Example: val{int64_val: 5017}, val{int64_val: 1003}
	entityValues []*types.Value

	// Feature names is 1:1 correspondence with featureValue, featureStatus, and timestamp
	featureNames []string

	featureValues   []*types.Value
	featureStatuses []serving.FieldStatus
	eventTimestamps []*timestamppb.Timestamp
	RequestContext  map[string]*types.RepeatedValue
}

type MemoryBuffer struct {
	logs []*Log
}

type LoggingService struct {
	memoryBuffer *MemoryBuffer
	logChannel   chan *Log
	fs           *feast.FeatureStore
}

func NewLoggingService(fs *feast.FeatureStore, logChannelCapacity int, startLogProcessing bool) *LoggingService {
	// start handler processes?
	loggingService := &LoggingService{
		logChannel: make(chan *Log, logChannelCapacity),
		memoryBuffer: &MemoryBuffer{
			logs: make([]*Log, 0),
		},
		fs: fs,
	}
	// For testing purposes, so we can test timeouts.
	if startLogProcessing {
		go loggingService.processLogs()
	}
	return loggingService
}

func (s *LoggingService) emitLog(log *Log) error {
	select {
	case s.logChannel <- log:
		return nil
	case <-time.After(20 * time.Millisecond):
		return fmt.Errorf("could not add to log channel with capacity %d. Current log channel length is %d", cap(s.logChannel), len(s.logChannel))
	}
}

func (s *LoggingService) processLogs() {
	// start a periodic flush
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case t := <-ticker.C:
			go s.flushLogsToOfflineStorage(t)
		case new_log := <-s.logChannel:
			log.Printf("Pushing %s to memory.\n", new_log.featureValues)
			s.memoryBuffer.logs = append(s.memoryBuffer.logs, new_log)
		}
	}
}

func (s *LoggingService) flushLogsToOfflineStorage(t time.Time) {
	//offlineStore := fs.config.OfflineStore["type"]
	// switch offlineStore{
	// case "file":
	// 	// call python??
	// case "snowflake":
	//
	// }
	//Do different row level manipulations and add to offline store
	log.Printf("Flushing buffer to offline storage with channel length: %d\n at time: "+t.String(), len(s.memoryBuffer.logs))
}
