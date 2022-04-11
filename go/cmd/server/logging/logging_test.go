package logging

import (
	"reflect"
	"testing"
	"time"

	"github.com/apache/arrow/go/v8/arrow"
	"github.com/apache/arrow/go/v8/arrow/array"
	"github.com/feast-dev/feast/go/internal/feast/model"
	"github.com/feast-dev/feast/go/internal/test"
	"github.com/feast-dev/feast/go/protos/feast/serving"
	"github.com/feast-dev/feast/go/protos/feast/types"
	gotypes "github.com/feast-dev/feast/go/types"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestLoggingChannelTimeout(t *testing.T) {
	// Pregenerated using `feast init`.
	loggingService, err := NewLoggingService(nil, 1, "", false)
	assert.Nil(t, err)
	assert.Empty(t, loggingService.memoryBuffer.logs)
	ts := timestamppb.New(time.Now())
	newLog := Log{
		FeatureStatuses: []serving.FieldStatus{serving.FieldStatus_PRESENT},
		EventTimestamps: []*timestamppb.Timestamp{ts, ts},
	}
	loggingService.EmitLog(&newLog)
	// Wait for memory buffer flush
	time.Sleep(20 * time.Millisecond)
	newTs := timestamppb.New(time.Now())

	newLog2 := Log{
		FeatureStatuses: []serving.FieldStatus{serving.FieldStatus_PRESENT},
		EventTimestamps: []*timestamppb.Timestamp{newTs, newTs},
	}
	err = loggingService.EmitLog(&newLog2)
	// The channel times out and doesn't hang.
	time.Sleep(20 * time.Millisecond)
	assert.NotNil(t, err)
}

func TestSchemaTypeRetrieval(t *testing.T) {
	featureService, entities, featureViews, odfvs := InitializeFeatureRepoVariablesForTest()
	schema, err := GetSchemaFromFeatureService(featureService, entities, featureViews, odfvs)
	assert.Nil(t, err)
	assert.Contains(t, schema.EntityTypes, "driver_id")
	assert.True(t, reflect.DeepEqual(schema.EntityTypes["driver_id"], types.ValueType_INT64))

	features := []string{"int64", "float32", "int32", "double"}
	types := []types.ValueType_Enum{*types.ValueType_INT64.Enum(), *types.ValueType_FLOAT.Enum(), *types.ValueType_INT32.Enum(), *types.ValueType_DOUBLE.Enum()}
	for idx, featureName := range features {
		assert.Contains(t, schema.FeaturesTypes, featureName)
		assert.Equal(t, schema.FeaturesTypes[featureName], types[idx])
	}
}

func TestSerializeToArrowTable(t *testing.T) {
	table, expectedSchema, expectedColumns, err := GenerateTestLogsAndConvertToArrowTable()
	assert.Nil(t, err)
	defer table.Release()
	tr := array.NewTableReader(table, -1)

	defer tr.Release()
	for tr.Next() {
		rec := tr.Record()
		assert.NotNil(t, rec)
		for _, field := range rec.Schema().Fields() {
			assert.Contains(t, expectedSchema, field.Name)
			assert.Equal(t, field.Type, expectedSchema[field.Name])
		}
		values, err := test.GetProtoFromRecord(rec)

		assert.Nil(t, err)
		for name, val := range values {
			assert.Equal(t, len(val.Val), len(expectedColumns[name].Val))
			for idx, featureVal := range val.Val {
				assert.Equal(t, featureVal.Val, expectedColumns[name].Val[idx].Val)
			}
		}
	}
}

// Initialize all dummy featureservice, entities and featureviews/on demand featureviews for testing.
func InitializeFeatureRepoVariablesForTest() (*model.FeatureService, []*model.Entity, []*model.FeatureView, []*model.OnDemandFeatureView) {
	f1 := model.NewFeature(
		"int64",
		types.ValueType_INT64,
	)
	f2 := model.NewFeature(
		"float32",
		types.ValueType_FLOAT,
	)
	projection1 := model.NewFeatureViewProjection(
		"featureView1",
		"",
		[]*model.Feature{f1, f2},
		map[string]string{},
	)
	baseFeatureView1 := model.CreateBaseFeatureView(
		"featureView1",
		[]*model.Feature{f1, f2},
		projection1,
	)
	featureView1 := model.CreateFeatureView(baseFeatureView1, nil, map[string]struct{}{})
	entity1 := model.CreateNewEntity("driver_id", types.ValueType_INT64, "driver_id")
	f3 := model.NewFeature(
		"int32",
		types.ValueType_INT32,
	)
	f4 := model.NewFeature(
		"double",
		types.ValueType_DOUBLE,
	)
	projection2 := model.NewFeatureViewProjection(
		"featureView2",
		"",
		[]*model.Feature{f3, f4},
		map[string]string{},
	)
	baseFeatureView2 := model.CreateBaseFeatureView(
		"featureView2",
		[]*model.Feature{f3, f4},
		projection2,
	)
	featureView2 := model.CreateFeatureView(baseFeatureView2, nil, map[string]struct{}{})
	featureService := model.NewFeatureService(
		"test_service",
		"test_project",
		nil,
		nil,
		[]*model.FeatureViewProjection{projection1, projection2},
	)
	return featureService, []*model.Entity{entity1}, []*model.FeatureView{featureView1, featureView2}, []*model.OnDemandFeatureView{}
}

// Create dummy FeatureService, Entities, and FeatureViews add them to the logger and convert the logs to Arrow table.
// Returns arrow table, expected test schema, and expected columns.
func GenerateTestLogsAndConvertToArrowTable() (array.Table, map[string]arrow.DataType, map[string]*types.RepeatedValue, error) {
	featureService, entities, featureViews, odfvs := InitializeFeatureRepoVariablesForTest()
	schema, err := GetSchemaFromFeatureService(featureService, entities, featureViews, odfvs)
	if err != nil {
		return nil, nil, nil, err
	}
	loggingService, err := NewLoggingService(nil, 2, "", false)
	if err != nil {
		return nil, nil, nil, err
	}
	ts := timestamppb.New(time.Now())
	log1 := Log{
		EntityValue: []*types.Value{
			{Val: &types.Value_Int64Val{Int64Val: 1001}},
		},
		FeatureValues: []*types.Value{
			{Val: &types.Value_Int64Val{Int64Val: 1000}},
			{Val: &types.Value_FloatVal{FloatVal: 0.64}},
			{Val: &types.Value_Int32Val{Int32Val: 55}},
			{Val: &types.Value_DoubleVal{DoubleVal: 0.97}},
		},
		FeatureStatuses: []serving.FieldStatus{
			serving.FieldStatus_PRESENT,
			serving.FieldStatus_PRESENT,
			serving.FieldStatus_PRESENT,
			serving.FieldStatus_PRESENT,
		},
		EventTimestamps: []*timestamppb.Timestamp{
			ts, ts, ts, ts,
		},
	}
	log2 := Log{
		EntityValue: []*types.Value{
			{Val: &types.Value_Int64Val{Int64Val: 1003}},
		},
		FeatureValues: []*types.Value{
			{Val: &types.Value_Int64Val{Int64Val: 1001}},
			{Val: &types.Value_FloatVal{FloatVal: 1.56}},
			{Val: &types.Value_Int32Val{Int32Val: 200}},
			{Val: &types.Value_DoubleVal{DoubleVal: 8.97}},
		},
		FeatureStatuses: []serving.FieldStatus{
			serving.FieldStatus_PRESENT,
			serving.FieldStatus_PRESENT,
			serving.FieldStatus_PRESENT,
			serving.FieldStatus_PRESENT,
		},
		EventTimestamps: []*timestamppb.Timestamp{
			ts, ts, ts, ts,
		},
	}

	expectedSchema := make(map[string]arrow.DataType)
	for joinKey, entityType := range schema.EntityTypes {
		arrowType, err := gotypes.ValueTypeEnumToArrowType(entityType)
		if err != nil {
			return nil, nil, nil, err
		}
		expectedSchema[joinKey] = arrowType
	}
	for featureName, featureType := range schema.FeaturesTypes {
		arrowType, err := gotypes.ValueTypeEnumToArrowType(featureType)
		if err != nil {
			return nil, nil, nil, err
		}
		expectedSchema[featureName] = arrowType
	}

	expectedColumns := map[string]*types.RepeatedValue{
		"driver_id": {
			Val: []*types.Value{
				log1.EntityValue[0],
				log2.EntityValue[0]},
		},
		"int64": {
			Val: []*types.Value{
				log1.FeatureValues[0],
				log2.FeatureValues[0]},
		},
		"float32": {
			Val: []*types.Value{
				log1.FeatureValues[1],
				log2.FeatureValues[1]},
		},
		"int32": {
			Val: []*types.Value{
				log1.FeatureValues[2],
				log2.FeatureValues[2]},
		},
		"double": {
			Val: []*types.Value{
				log1.FeatureValues[3],
				log2.FeatureValues[3]},
		},
	}

	dummyTicker := time.NewTicker(10 * time.Second)
	// stop the ticker so that the logs are not flushed to offline storage
	dummyTicker.Stop()
	loggingService.EmitLog(&log1)

	loggingService.EmitLog(&log2)
	loggingService.HandleLogFlushing(dummyTicker)
	loggingService.HandleLogFlushing(dummyTicker)
	table, err := ConvertMemoryBufferToArrowTable(loggingService.memoryBuffer, schema)
	if err != nil {
		return nil, nil, nil, err
	}
	return table, expectedSchema, expectedColumns, nil
}
