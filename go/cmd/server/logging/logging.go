package logging

import (
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/apache/arrow/go/v8/arrow"
	"github.com/apache/arrow/go/v8/arrow/array"
	"github.com/apache/arrow/go/v8/arrow/memory"
	"github.com/feast-dev/feast/go/internal/feast"
	"github.com/feast-dev/feast/go/protos/feast/serving"
	"github.com/feast-dev/feast/go/protos/feast/types"
	gotypes "github.com/feast-dev/feast/go/types"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type Log struct {
	// Example: val{int64_val: 5017}, val{int64_val: 1003}
	EntityValue []*types.Value

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

		//s.offlineLogStorage.FlushToStorage(s.memoryBuffer)
		//Clean memory buffer
		// Convert row level logs array in memory buffer to an array table in columnar
		// entities, fvs, odfvs, err := s.GetFcos()
		// if err != nil {
		// 	return nil, err
		// }
		// fcoSchema, err := GetTypesFromFeatureService(s.memoryBuffer.featureService, entities, fvs, odfvs)
		// if err != nil {
		// 	return nil, err
		// }
		// table, err := s.getLogInArrowTable(fcoSchema)
		s.memoryBuffer.logs = s.memoryBuffer.logs[:0]
	} else {
		// Currently don't support any other offline flushing.
		return errors.New("currently only file type is supported for offline log storage")
	}
	return nil
}

func (s *LoggingService) GetLogInArrowTable(fcoSchema *Schema) (array.Table, error) {
	// Memory Buffer is a
	if s.memoryBuffer.featureService == nil {
		return nil, errors.New("no Feature Service in logging service instantiated")
	}

	arrowMemory := memory.NewGoAllocator()

	columnNameToProtoValueArray := make(map[string][]*types.Value)
	columnNameToStatus := make(map[string][]int32)
	columnNameToTimestamp := make(map[string][]int64)
	entityNameToEntityValues := make(map[string][]*types.Value)
	for _, log := range s.memoryBuffer.logs {
		// TODO(kevjumba) get it from the featureview
		// Get the entities from the feature view
		// Grab the corresponding join keys and then process using joinkey map to get the entity key related to the features
		// For each entity key create a new column
		//  populate entitynametoentityvalues map
		for entityName, idAndType := range fcoSchema.EntityTypes {
			if _, ok := entityNameToEntityValues[entityName]; !ok {
				entityNameToEntityValues[entityName] = make([]*types.Value, 0)
			}
			entityNameToEntityValues[entityName] = append(entityNameToEntityValues[entityName], log.EntityValue[idAndType.index])
		}

		for featureName, idAndType := range fcoSchema.FeaturesTypes {
			// populate the proto value arrays with values from memory buffer in separate columns one for each feature name
			if _, ok := columnNameToProtoValueArray[featureName]; !ok {
				columnNameToProtoValueArray[featureName] = make([]*types.Value, 0)
				columnNameToStatus[featureName] = make([]int32, 0)
				columnNameToTimestamp[featureName] = make([]int64, 0)
			}
			columnNameToProtoValueArray[featureName] = append(columnNameToProtoValueArray[featureName], log.FeatureValues[idAndType.index])
			columnNameToStatus[featureName] = append(columnNameToStatus[featureName], int32(log.FeatureStatuses[idAndType.index]))
			columnNameToTimestamp[featureName] = append(columnNameToTimestamp[featureName], log.EventTimestamps[idAndType.index].AsTime().UnixNano()/int64(time.Millisecond))
		}
	}

	fields := make([]arrow.Field, 0)
	columns := make([]array.Interface, 0)
	for name, val := range entityNameToEntityValues {
		valArrowArray, err := gotypes.ProtoValuesToArrowArray(val, arrowMemory, len(columnNameToProtoValueArray))
		if err != nil {
			return nil, err
		}
		fields = append(fields, arrow.Field{
			Name: name,
			Type: valArrowArray.DataType(),
		})
		columns = append(columns, valArrowArray)
	}

	for featureName, _ := range fcoSchema.FeaturesTypes {

		proto_arr := columnNameToProtoValueArray[featureName]

		arrowArray, err := gotypes.ProtoValuesToArrowArray(proto_arr, arrowMemory, len(columnNameToProtoValueArray))
		if err != nil {
			return nil, err
		}

		fields = append(fields, arrow.Field{
			Name: featureName,
			Type: arrowArray.DataType(),
		})
		columns = append(columns, arrowArray)
	}
	schema := arrow.NewSchema(
		fields,
		nil,
	)
	result := array.Record(array.NewRecord(schema, columns, int64(len(s.memoryBuffer.logs))))
	// create an arrow table -> write this to parquet.

	tbl := array.NewTableFromRecords(schema, []array.Record{result})
	// arrow table -> write this to parquet
	return array.Table(tbl), nil
}

type Schema struct {
	EntityTypes      map[string]*IndexAndType
	FeaturesTypes    map[string]*IndexAndType
	RequestDataTypes map[string]*IndexAndType
}

type IndexAndType struct {
	dtype types.ValueType_Enum
	// index of the fco(entity, feature, request) as it is ordered in logs
	index int
}

func GetTypesFromFeatureService(featureService *feast.FeatureService, entities []*feast.Entity, featureViews []*feast.FeatureView, onDemandFeatureViews []*feast.OnDemandFeatureView) (*Schema, error) {
	fvs := make(map[string]*feast.FeatureView)
	odFvs := make(map[string]*feast.OnDemandFeatureView)

	//featureViews, err := fs.listFeatureViews(hideDummyEntity)

	for _, featureView := range featureViews {
		fvs[featureView.Base.Name] = featureView
	}

	for _, onDemandFeatureView := range onDemandFeatureViews {
		odFvs[onDemandFeatureView.Base.Name] = onDemandFeatureView
	}

	entityJoinKeyToType := make(map[string]*IndexAndType)
	entityNameToJoinKeyMap := make(map[string]string)
	for idx, entity := range entities {
		entityNameToJoinKeyMap[entity.Name] = entity.Joinkey
		entityJoinKeyToType[entity.Joinkey] = &IndexAndType{
			dtype: entity.Valuetype,
			index: idx,
		}
	}
	allFeatureTypes := make(map[string]*IndexAndType)
	//allRequestDataTypes := make(map[string]*types.ValueType_Enum)
	featureIndex := 0
	for _, featureProjection := range featureService.Projections {
		// Create copies of FeatureView that may contains the same *FeatureView but
		// each differentiated by a *FeatureViewProjection
		featureViewName := featureProjection.Name
		if fv, ok := fvs[featureViewName]; ok {
			for _, f := range fv.Base.Features {
				// add feature to map
				allFeatureTypes[f.Name] = &IndexAndType{
					dtype: f.Dtype,
					index: featureIndex,
				}
				featureIndex += 1
			}
		} else if _, ok := odFvs[featureViewName]; ok {
			// append this -> odfv.getRequestDataSchema() all request data schema
			// allRequestDataTypes[featureViewName] = odfv.
			featureIndex += 1
		} else {
			return nil, fmt.Errorf("no such feature view found in feature service %s", featureViewName)
		}
	}
	schema := &Schema{
		EntityTypes:      entityJoinKeyToType,
		FeaturesTypes:    allFeatureTypes,
		RequestDataTypes: nil,
	}
	return schema, nil
}

func (s *LoggingService) GetFcos() ([]*feast.Entity, []*feast.FeatureView, []*feast.OnDemandFeatureView, error) {
	odfvs, err := s.fs.ListOnDemandFeatureViews()
	if err != nil {
		return nil, nil, nil, err
	}
	fvs, err := s.fs.ListFeatureViews()
	if err != nil {
		return nil, nil, nil, err
	}
	entities, err := s.fs.ListEntities(false)
	if err != nil {
		return nil, nil, nil, err
	}
	return entities, fvs, odfvs, nil
}
