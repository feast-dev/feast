package main

import (
	"log"
	"time"

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
	logs []Log
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
		// log.Println(request.Entities)
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
		s.logChannel <- newLog
	}
}

func processLogs(log_channel chan Log, logBuffer *MemoryBuffer) {
	// start a periodic flush
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case t := <-ticker.C:
			log.Printf("time is %d", t)
			log.Printf("Flushing buffer to offline storage with channel length: %d\n", len(logBuffer.logs))
		case new_log := <-log_channel:
			log.Printf("Pushing %s to memory.\n", new_log.featureValues)
			logBuffer.logs = append(logBuffer.logs, new_log)
		}
	}
}
