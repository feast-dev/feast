package logging

import (
	"path/filepath"
	"reflect"
	"runtime"
	"testing"
	"time"

	gotypes "github.com/feast-dev/feast/go/types"

	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/array"
	"github.com/feast-dev/feast/go/internal/feast"
	"github.com/feast-dev/feast/go/protos/feast/serving"
	"github.com/feast-dev/feast/go/protos/feast/types"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Return absolute path to the test_repo directory regardless of the working directory
func getRepoPath(basePath string) string {
	// Get the file path of this source file, regardless of the working directory
	if basePath == "" {
		_, filename, _, ok := runtime.Caller(0)
		if !ok {
			panic("couldn't find file path of the test file")
		}
		return filepath.Join(filename, "..", "..", "feature_repo")
	} else {
		return filepath.Join(basePath, "feature_repo")
	}
}

func TestLoggingChannelTimeout(t *testing.T) {
	// Pregenerated using `feast init`.
	loggingService, err := NewLoggingService(nil, 1, false)
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
	schema, err := GetTypesFromFeatureService(featureService, entities, featureViews, odfvs)
	assert.Nil(t, err)
	assert.Contains(t, schema.EntityTypes, "driver_id")
	assert.True(t, reflect.DeepEqual(schema.EntityTypes["driver_id"], &IndexAndType{
		dtype: types.ValueType_INT64,
		index: 0,
	}))

	features := []string{"int64", "float32", "int32", "double"}
	for idx, featureName := range features {
		assert.Contains(t, schema.FeaturesTypes, featureName)
		assert.Equal(t, schema.FeaturesTypes[featureName].index, idx)
	}

}

func TestSerializeToArrowTable(t *testing.T) {
	featureService, entities, featureViews, odfvs := InitializeFeatureRepoVariablesForTest()
	schema, err := GetTypesFromFeatureService(featureService, entities, featureViews, odfvs)
	assert.Nil(t, err)
	loggingService, err := NewLoggingService(nil, 1, false)
	assert.Nil(t, err)
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
	memoryBuffer := &MemoryBuffer{
		logs:           []*Log{&log1, &log2},
		featureService: featureService,
	}
	loggingService.memoryBuffer = memoryBuffer
	table, err := loggingService.getLogInArrowTable(schema)
	defer table.Release()
	tr := array.NewTableReader(table, -1)
	expected_schema := map[string]arrow.DataType{
		"driver_id": arrow.PrimitiveTypes.Int64,
		"int32":     arrow.PrimitiveTypes.Int32,
		"double":    arrow.PrimitiveTypes.Float64,
		"int64":     arrow.PrimitiveTypes.Int64,
		"float32":   arrow.PrimitiveTypes.Float32,
	}

	expected_columns := map[string]*types.RepeatedValue{
		"double": {
			Val: []*types.Value{{Val: &types.Value_DoubleVal{DoubleVal: 0.97}},
				{Val: &types.Value_DoubleVal{DoubleVal: 8.97}}}},
		"driver_id": {
			Val: []*types.Value{{Val: &types.Value_Int64Val{Int64Val: 1001}},
				{Val: &types.Value_Int64Val{Int64Val: 1003}}}},
		"float32": {
			Val: []*types.Value{{Val: &types.Value_FloatVal{FloatVal: 0.64}},
				{Val: &types.Value_FloatVal{FloatVal: 1.56}}}},
		"int32": {
			Val: []*types.Value{{Val: &types.Value_Int32Val{Int32Val: 55}},
				{Val: &types.Value_Int32Val{Int32Val: 200}}}},
		"int64": {
			Val: []*types.Value{{Val: &types.Value_Int64Val{Int64Val: 1000}},
				{Val: &types.Value_Int64Val{Int64Val: 1001}}}},
	}

	defer tr.Release()
	for tr.Next() {
		rec := tr.Record()
		assert.NotNil(t, rec)
		for _, field := range rec.Schema().Fields() {
			assert.Contains(t, expected_schema, field.Name)
			assert.Equal(t, field.Type, expected_schema[field.Name])
		}
		values, err := GetProtoFromRecord(rec)

		assert.Nil(t, err)
		assert.True(t, reflect.DeepEqual(values, expected_columns))
	}

	assert.Nil(t, err)
}
func GetProtoFromRecord(rec array.Record) (map[string]*types.RepeatedValue, error) {
	r := make(map[string]*types.RepeatedValue)
	schema := rec.Schema()
	for idx, column := range rec.Columns() {
		field := schema.Field(idx)
		values, err := gotypes.ArrowValuesToProtoValues(column)
		if err != nil {
			return nil, err
		}
		r[field.Name] = &types.RepeatedValue{Val: values}
	}
	return r, nil
}
func InitializeFeatureRepoVariablesForTest() (*feast.FeatureService, []*feast.Entity, []*feast.FeatureView, []*feast.OnDemandFeatureView) {
	f1 := feast.NewFeature(
		"int64",
		types.ValueType_INT64,
	)
	f2 := feast.NewFeature(
		"float32",
		types.ValueType_FLOAT,
	)
	projection1 := feast.NewFeatureViewProjection(
		"featureView1",
		"",
		[]*feast.Feature{f1, f2},
		map[string]string{},
	)
	baseFeatureView1 := feast.CreateBaseFeatureView(
		"featureView1",
		[]*feast.Feature{f1, f2},
		projection1,
	)
	featureView1 := feast.CreateFeatureView(baseFeatureView1, nil, map[string]struct{}{})
	entity1 := feast.CreateNewEntity("driver_id", types.ValueType_INT64, "driver_id")
	f3 := feast.NewFeature(
		"int32",
		types.ValueType_INT32,
	)
	f4 := feast.NewFeature(
		"double",
		types.ValueType_DOUBLE,
	)
	projection2 := feast.NewFeatureViewProjection(
		"featureView2",
		"",
		[]*feast.Feature{f3, f4},
		map[string]string{},
	)
	baseFeatureView2 := feast.CreateBaseFeatureView(
		"featureView2",
		[]*feast.Feature{f3, f4},
		projection2,
	)
	featureView2 := feast.CreateFeatureView(baseFeatureView2, nil, map[string]struct{}{})
	featureService := feast.NewFeatureService(
		"test_service",
		"test_project",
		nil,
		nil,
		[]*feast.FeatureViewProjection{projection1, projection2},
	)
	return featureService, []*feast.Entity{entity1}, []*feast.FeatureView{featureView1, featureView2}, []*feast.OnDemandFeatureView{}
}
