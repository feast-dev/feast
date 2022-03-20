package main

import (
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
	logChannel   chan Log
	fs           *feast.FeatureStore
}

func NewLoggingService(fs *feast.FeatureStore) *LoggingService {
	// start handler processes?
	loggingService := &LoggingService{
		logChannel: make(chan Log, 1000),
		memoryBuffer: &MemoryBuffer{
			logs: make([]*Log, 0),
		},
		fs: fs,
	}
	go loggingService.processLogs()
	return loggingService
}

func flushToChannel(s *servingServiceServer, onlineResponse *serving.GetOnlineFeaturesResponse, request *serving.GetOnlineFeaturesRequest) {
	for _, featureVector := range onlineResponse.Results {
		featureValues := make([]*types.Value, len(featureVector.Values))
		eventTimestamps := make([]*timestamppb.Timestamp, len(featureVector.EventTimestamps))
		for idx, featureValue := range featureVector.Values {
			if featureVector.Statuses[idx] != serving.FieldStatus_PRESENT {
				continue
			}
			featureValues[idx] = &types.Value{Val: featureValue.Val}
		}
		for idx, ts := range featureVector.EventTimestamps {
			if featureVector.Statuses[idx] != serving.FieldStatus_PRESENT {
				continue
			}
			eventTimestamps[idx] = &timestamppb.Timestamp{Seconds: ts.Seconds, Nanos: ts.Nanos}
		}
		// TODO(kevjumba): For filtering out and extracting entity names when the bug with join keys is fixed.
		// for idx, featureName := range onlineResponse.Metadata.FeatureNames.Val {
		// 	if _, ok := request.Entities[featureName]; ok {
		// 		entityNames = append(entityNames, featureName)
		// 		entityValues = append(entityValues, featureVector.Values[idx])
		// 	} else {
		// 		featureNames = append(featureNames, onlineResponse.Metadata.FeatureNames.Val[idx:]...)
		// 		featureValues = append(featureValues, featureVector.Values[idx:]...)
		// 		featureStatuses = append(featureStatuses, featureVector.Statuses[idx:]...)
		// 		eventTimestamps = append(eventTimestamps, featureVector.EventTimestamps[idx:]...)
		// 		break
		// 	}
		// }
		// featureNames = append(featureNames, onlineResponse.Metadata.FeatureNames.Val[:]...)
		// featureValues = append(featureValues, featureVector.Values[:]...)
		// featureStatuses = append(featureStatuses, featureVector.Statuses[:]...)
		// eventTimestamps = append(eventTimestamps, featureVector.EventTimestamps[:]...)

		newLog := Log{
			featureNames:    onlineResponse.Metadata.FeatureNames.Val,
			featureValues:   featureValues,
			featureStatuses: featureVector.Statuses,
			eventTimestamps: eventTimestamps,
			// TODO(kevjumba): figure out if this is required
			RequestContext: request.RequestContext,
		}
		s.loggingService.logChannel <- newLog
	}
}

func (s *LoggingService) processLogs() {
	// start a periodic flush
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case t := <-ticker.C:
			s.flushLogsToOfflineStorage(t)
		case new_log := <-s.logChannel:
			log.Printf("Pushing %s to memory.\n", new_log.featureValues)
			s.memoryBuffer.logs = append(s.memoryBuffer.logs, &new_log)
			time.Sleep(110 * time.Millisecond)
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
