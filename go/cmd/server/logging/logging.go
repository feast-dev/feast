package logging

import (
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/apache/arrow/go/arrow/array"
	"github.com/feast-dev/feast/go/internal/feast"
	"github.com/feast-dev/feast/go/protos/feast/serving"
	"github.com/feast-dev/feast/go/protos/feast/types"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type Log struct {
	// Example: driver_id, customer_id
	EntityName string
	// Example: val{int64_val: 5017}, val{int64_val: 1003}
	EntityValue *types.Value

	// Feature names is 1:1 correspondence with featureValue, featureStatus, and timestamp
	FeatureNames []string

	FeatureValues   []*types.Value
	FeatureStatuses []serving.FieldStatus
	EventTimestamps []*timestamppb.Timestamp
	RequestContext  map[string]*types.RepeatedValue
}

// driver_id,
// 1003, 1004
// [acc rate conv rate avg_daily_trips]
// [entityvalues, acc_rate conv_rate avg_daily_trips, acc_ratestatus, conv_rate_status]
// [entityvalues, entity value]

type MemoryBuffer struct {
	featureService *feast.FeatureService
	logs           []*Log
}

type LoggingService struct {
	memoryBuffer      *MemoryBuffer
	logChannel        chan *Log
	fs                *feast.FeatureStore
	offlineLogStorage OfflineLogStorage
	enableLogging     bool
}

func NewLoggingService(fs *feast.FeatureStore, logChannelCapacity int, enableLogging bool) (*LoggingService, error) {
	// start handler processes?
	loggingService := &LoggingService{
		logChannel: make(chan *Log, logChannelCapacity),
		memoryBuffer: &MemoryBuffer{
			logs: make([]*Log, 0),
		},
		enableLogging: enableLogging,
		fs:            fs,
	}
	if !enableLogging || fs == nil {
		loggingService.offlineLogStorage = nil
	} else {
		offlineLogStorage, err := NewOfflineStore(fs.GetRepoConfig())
		loggingService.offlineLogStorage = offlineLogStorage

		if err != nil {
			return nil, err
		}
		// Start goroutine to process logs
		go loggingService.processLogs()
	}
	return loggingService, nil
}

func (s *LoggingService) EmitLog(log *Log) error {
	select {
	case s.logChannel <- log:
		return nil
	case <-time.After(20 * time.Millisecond):
		return fmt.Errorf("could not add to log channel with capacity %d. Current log channel length is %d", cap(s.logChannel), len(s.logChannel))
	}
}

func (s *LoggingService) processLogs() {
	// start a periodic flush
	// TODO(kevjumba): set param so users can configure flushing duration
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case t := <-ticker.C:
			go s.flushLogsToOfflineStorage(t)
		case new_log := <-s.logChannel:
			log.Printf("Pushing %s to memory.\n", new_log.FeatureValues)
			s.memoryBuffer.logs = append(s.memoryBuffer.logs, new_log)
		}
	}
}

func (s *LoggingService) flushLogsToOfflineStorage(t time.Time) error {
	log.Printf("Flushing buffer to offline storage with channel length: %d\n at time: "+t.String(), len(s.memoryBuffer.logs))
	if !s.enableLogging {
		return nil
	}
	offlineStoreType, ok := getOfflineStoreType(s.fs.GetRepoConfig().OfflineStore)
	if !ok {
		return fmt.Errorf("could not get offline storage type for config: %s", s.fs.GetRepoConfig().OfflineStore)
	}
	if offlineStoreType == "file" {

		s.offlineLogStorage.FlushToStorage(s.memoryBuffer)
		//Clean memory buffer
		s.memoryBuffer.logs = s.memoryBuffer.logs[:0]
	} else {
		// Currently don't support any other offline flushing.
		return errors.New("currently only file type is supported for offline log storage")
	}
	return nil
}

func (s *LoggingService) getLogInArrowTable(memoryBuffer *MemoryBuffer) (*array.Table, error) {
	// input memoryBuffer -> featureColumns
	// map[string]*type.Value

	// fields := make([]*arrow.Field, 0)
	// columns := make([]array.Interface, 0)
	// for idx, feature := range featureService.features {
	// 	feature.Name

	// 	[]*proto.Value = columnNameToProtoValue[feature.Name]
	// 	arrowArray := types.ProtoValuesToArrowArray(protoValues)

	// 	fields = append(fields, &arrow.Field{
	// 		Name: feature.Name,
	// 		Type: arrowArray.DataType(),
	// 	})
	// 	columns = append(columns, arrowArray)
	// }

	// table := array.NewTable(
	// 	arrow.NewSchema(fields, nil),
	// 	columns
	// 	)

	// pqarrow.WriteTable(table)
}
