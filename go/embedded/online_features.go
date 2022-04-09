package embedded

import (
	"context"
	"fmt"
	"github.com/apache/arrow/go/v8/arrow"
	"github.com/apache/arrow/go/v8/arrow/array"
	"github.com/apache/arrow/go/v8/arrow/cdata"
	"github.com/apache/arrow/go/v8/arrow/memory"
	"github.com/feast-dev/feast/go/internal/feast"
	"github.com/feast-dev/feast/go/internal/feast/model"
	"github.com/feast-dev/feast/go/internal/feast/onlineserving"
	"github.com/feast-dev/feast/go/internal/feast/registry"
	"github.com/feast-dev/feast/go/internal/feast/transformation"
	prototypes "github.com/feast-dev/feast/go/protos/feast/types"
	"github.com/feast-dev/feast/go/types"
	"log"
)

type OnlineFeatureService struct {
	fs *feast.FeatureStore
}

type OnlineFeatureServiceConfig struct {
	RepoPath   string
	RepoConfig string
}

type DataTable struct {
	DataPtr   uintptr
	SchemaPtr uintptr
}

func NewOnlineFeatureService(conf *OnlineFeatureServiceConfig, transformationCallback transformation.TransformationCallback) *OnlineFeatureService {
	repoConfig, err := registry.NewRepoConfigFromJSON(conf.RepoPath, conf.RepoConfig)
	if err != nil {
		log.Fatalln(err)
	}

	fs, err := feast.NewFeatureStore(repoConfig, transformationCallback)
	if err != nil {
		log.Fatalln(err)
	}

	return &OnlineFeatureService{fs: fs}
}

func (s *OnlineFeatureService) GetEntityTypesMap(featureRefs []string) (map[string]int32, error) {
	viewNames := make(map[string]interface{})
	for _, featureRef := range featureRefs {
		viewName, _, err := onlineserving.ParseFeatureReference(featureRef)
		if err != nil {
			return nil, err
		}
		viewNames[viewName] = nil
	}

	entities, _ := s.fs.ListEntities(true)
	entitiesByName := make(map[string]*model.Entity)
	for _, entity := range entities {
		entitiesByName[entity.Name] = entity
	}

	joinKeyTypes := make(map[string]int32)

	for viewName := range viewNames {
		view, err := s.fs.GetFeatureView(viewName, true)
		if err != nil {
			// skip on demand feature views
			continue
		}
		for entityName := range view.Entities {
			entity := entitiesByName[entityName]
			joinKeyTypes[entity.JoinKey] = int32(entity.ValueType.Number())
		}
	}

	return joinKeyTypes, nil
}

func (s *OnlineFeatureService) GetEntityTypesMapByFeatureService(featureServiceName string) (map[string]int32, error) {
	featureService, err := s.fs.GetFeatureService(featureServiceName)
	if err != nil {
		return nil, err
	}

	joinKeyTypes := make(map[string]int32)

	entities, _ := s.fs.ListEntities(true)
	entitiesByName := make(map[string]*model.Entity)
	for _, entity := range entities {
		entitiesByName[entity.Name] = entity
	}

	for _, projection := range featureService.Projections {
		view, err := s.fs.GetFeatureView(projection.Name, true)
		if err != nil {
			// skip on demand feature views
			continue
		}
		for entityName := range view.Entities {
			entity := entitiesByName[entityName]
			joinKeyTypes[entity.JoinKey] = int32(entity.ValueType.Number())
		}
	}

	return joinKeyTypes, nil
}

func (s *OnlineFeatureService) GetOnlineFeatures(
	featureRefs []string,
	featureServiceName string,
	entities DataTable,
	requestData DataTable,
	fullFeatureNames bool,
	output DataTable) error {

	entitiesRecord, err := readArrowRecord(entities)
	if err != nil {
		return err
	}

	numRows := entitiesRecord.Column(0).Len()

	entitiesProto, err := recordToProto(entitiesRecord)
	if err != nil {
		return err
	}

	requestDataRecords, err := readArrowRecord(requestData)
	if err != nil {
		return err
	}

	requestDataProto, err := recordToProto(requestDataRecords)
	if err != nil {
		return err
	}

	var featureService *model.FeatureService
	if featureServiceName != "" {
		featureService, err = s.fs.GetFeatureService(featureServiceName)
	}

	resp, err := s.fs.GetOnlineFeatures(
		context.Background(),
		featureRefs,
		featureService,
		entitiesProto,
		requestDataProto,
		fullFeatureNames)

	if err != nil {
		return err
	}

	outputFields := make([]arrow.Field, 0)
	outputColumns := make([]arrow.Array, 0)
	pool := memory.NewGoAllocator()
	for _, featureVector := range resp {
		outputFields = append(outputFields,
			arrow.Field{
				Name: featureVector.Name,
				Type: featureVector.Values.DataType()})
		outputFields = append(outputFields,
			arrow.Field{
				Name: fmt.Sprintf("%s__status", featureVector.Name),
				Type: arrow.PrimitiveTypes.Int32})
		outputFields = append(outputFields,
			arrow.Field{
				Name: fmt.Sprintf("%s__timestamp", featureVector.Name),
				Type: arrow.PrimitiveTypes.Int64})

		outputColumns = append(outputColumns, featureVector.Values)

		statusColumnBuilder := array.NewInt32Builder(pool)
		for _, status := range featureVector.Statuses {
			statusColumnBuilder.Append(int32(status))
		}
		statusColumn := statusColumnBuilder.NewArray()
		outputColumns = append(outputColumns, statusColumn)

		tsColumnBuilder := array.NewInt64Builder(pool)
		for _, ts := range featureVector.Timestamps {
			tsColumnBuilder.Append(ts.GetSeconds())
		}
		tsColumn := tsColumnBuilder.NewArray()
		outputColumns = append(outputColumns, tsColumn)
	}

	result := array.NewRecord(arrow.NewSchema(outputFields, nil), outputColumns, int64(numRows))

	cdata.ExportArrowRecordBatch(result,
		cdata.ArrayFromPtr(output.DataPtr),
		cdata.SchemaFromPtr(output.SchemaPtr))

	return nil
}

/*
	Read Record Batch from memory managed by Python caller.
	Python part uses C ABI interface to export this record into C Data Interface,
	and then it provides pointers (dataPtr & schemaPtr) to the Go part.
	Here we import this data from given pointers and wrap the underlying values
	into Go Arrow Interface (array.Record).
	See export code here https://github.com/feast-dev/feast/blob/master/sdk/python/feast/embedded_go/online_features_service.py
*/
func readArrowRecord(data DataTable) (array.Record, error) {
	return cdata.ImportCRecordBatch(
		cdata.ArrayFromPtr(data.DataPtr),
		cdata.SchemaFromPtr(data.SchemaPtr))
}

func recordToProto(rec array.Record) (map[string]*prototypes.RepeatedValue, error) {
	r := make(map[string]*prototypes.RepeatedValue)
	schema := rec.Schema()
	for idx, column := range rec.Columns() {
		field := schema.Field(idx)
		values, err := types.ArrowValuesToProtoValues(column)
		if err != nil {
			return nil, err
		}
		r[field.Name] = &prototypes.RepeatedValue{Val: values}
	}
	return r, nil
}
