package embedded

import (
	"context"
	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/array"
	"github.com/apache/arrow/go/arrow/cdata"
	"github.com/feast-dev/feast/go/internal/feast"
	"github.com/feast-dev/feast/go/protos/feast/types"
	"github.com/feast-dev/feast/go/utils"
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

func NewOnlineFeatureService(conf *OnlineFeatureServiceConfig) *OnlineFeatureService {
	repoConfig, err := feast.NewRepoConfigFromJSON(conf.RepoPath, conf.RepoConfig)
	if err != nil {
		log.Fatalln(err)
	}

	fs, err := feast.NewFeatureStore(repoConfig)
	if err != nil {
		log.Fatalln(err)
	}
	defer fs.DestructOnlineStore()
	return &OnlineFeatureService{fs: fs}
}

func (s *OnlineFeatureService) GetOnlineFeatures(
	featureRefs []string,
	featureServiceName string,
	entities DataTable,
	fullFeatureNames bool,
	projectName string,
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

	var featureService *feast.FeatureService
	if featureServiceName != "" {
		featureService, err = s.fs.GetFeatureService(featureServiceName, projectName)
	}

	resp, err := s.fs.GetOnlineFeatures(
		context.Background(),
		featureRefs,
		featureService,
		entitiesProto,
		fullFeatureNames)

	if err != nil {
		return err
	}

	outputFields := entitiesRecord.Schema().Fields()
	outputColumns := entitiesRecord.Columns()
	for _, featureVector := range resp {
		outputFields = append(outputFields,
			arrow.Field{Name: featureVector.Name, Type: featureVector.Values.DataType()})
		outputColumns = append(outputColumns, featureVector.Values)
	}

	result := array.NewRecord(arrow.NewSchema(outputFields, nil), outputColumns, int64(numRows))

	cdata.ExportArrowRecordBatch(result,
		cdata.ArrayFromPtr(output.DataPtr),
		cdata.SchemaFromPtr(output.SchemaPtr))

	return nil
}

func readArrowRecord(data DataTable) (array.Record, error) {
	return cdata.ImportCRecordBatch(
		cdata.ArrayFromPtr(data.DataPtr),
		cdata.SchemaFromPtr(data.SchemaPtr))
}

func recordToProto(rec array.Record) (map[string]*types.RepeatedValue, error) {
	r := make(map[string]*types.RepeatedValue)
	schema := rec.Schema()
	for idx, column := range rec.Columns() {
		field := schema.Field(idx)
		values, err := utils.ArrowValuesToProtoValues(column)
		if err != nil {
			return nil, err
		}
		r[field.Name] = &types.RepeatedValue{Val: values}
	}
	return r, nil
}
