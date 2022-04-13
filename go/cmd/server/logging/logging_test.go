package logging

import (
	"math/rand"
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
	newTs := timestamppb.New(time.Now())

	newLog2 := Log{
		FeatureStatuses: []serving.FieldStatus{serving.FieldStatus_PRESENT},
		EventTimestamps: []*timestamppb.Timestamp{newTs, newTs},
	}
	err = loggingService.EmitLog(&newLog2)
	// The channel times out and doesn't hang.
	assert.NotNil(t, err)
}

func TestSchemaTypeRetrieval(t *testing.T) {
	featureService, entities, featureViews, odfvs := InitializeFeatureRepoVariablesForTest()
	entityMap := make(map[string]*model.Entity)
	expectedEntityNames := make([]string, 0)
	expectedFeatureNames := make([]string, 0)
	for _, entity := range entities {
		entityMap[entity.Name] = entity
		expectedEntityNames = append(expectedEntityNames, entity.Name)
	}
	for _, featureView := range featureViews {
		for _, f := range featureView.Base.Features {
			expectedFeatureNames = append(expectedFeatureNames, GetFullFeatureName(featureView.Base.Name, f.Name))
		}
	}
	for _, featureView := range odfvs {
		for _, f := range featureView.Base.Features {
			expectedFeatureNames = append(expectedFeatureNames, GetFullFeatureName(featureView.Base.Name, f.Name))
		}
	}

	schema, err := GetSchemaFromFeatureService(featureService, entityMap, featureViews, odfvs)
	assert.Nil(t, err)

	assert.Equal(t, expectedFeatureNames, schema.Features)
	assert.Equal(t, expectedEntityNames, schema.Entities)
	for _, entityName := range expectedEntityNames {
		assert.Contains(t, schema.EntityTypes, entityName)
	}
	assert.True(t, reflect.DeepEqual(schema.EntityTypes["driver_id"], types.ValueType_INT64))

	types := []types.ValueType_Enum{*types.ValueType_INT64.Enum(), *types.ValueType_FLOAT.Enum(), *types.ValueType_INT32.Enum(), *types.ValueType_DOUBLE.Enum(), *types.ValueType_INT32.Enum(), *types.ValueType_DOUBLE.Enum()}
	for idx, featureName := range expectedFeatureNames {
		assert.Contains(t, schema.FeaturesTypes, featureName)
		assert.Equal(t, schema.FeaturesTypes[featureName], types[idx])
	}
}

func TestSchemaRetrievalIgnoresEntitiesNotInFeatureService(t *testing.T) {
	featureService, entities, featureViews, odfvs := InitializeFeatureRepoVariablesForTest()
	//Remove entities in featureservice
	for _, featureView := range featureViews {
		featureView.EntitiesMap = make(map[string]struct{})
	}
	entityMap := make(map[string]*model.Entity)
	for _, entity := range entities {
		entityMap[entity.Name] = entity
	}
	schema, err := GetSchemaFromFeatureService(featureService, entityMap, featureViews, odfvs)
	assert.Nil(t, err)
	assert.Empty(t, schema.EntityTypes)
}

func TestSchemaUsesOrderInFeatureService(t *testing.T) {
	featureService, entities, featureViews, odfvs := InitializeFeatureRepoVariablesForTest()
	expectedEntityNames := make([]string, 0)
	expectedFeatureNames := make([]string, 0)
	entityMap := make(map[string]*model.Entity)
	for _, entity := range entities {
		entityMap[entity.Name] = entity
	}
	for _, entity := range entities {
		entityMap[entity.Name] = entity
		expectedEntityNames = append(expectedEntityNames, entity.Name)
	}
	// Source of truth for order of featureNames
	for _, featureView := range featureViews {
		for _, f := range featureView.Base.Features {
			expectedFeatureNames = append(expectedFeatureNames, GetFullFeatureName(featureView.Base.Name, f.Name))
		}
	}
	for _, featureView := range odfvs {
		for _, f := range featureView.Base.Features {
			expectedFeatureNames = append(expectedFeatureNames, GetFullFeatureName(featureView.Base.Name, f.Name))
		}
	}

	rand.Seed(time.Now().UnixNano())
	// Shuffle the featureNames in incorrect order
	for _, featureView := range featureViews {
		rand.Shuffle(len(featureView.Base.Features), func(i, j int) {
			featureView.Base.Features[i], featureView.Base.Features[j] = featureView.Base.Features[j], featureView.Base.Features[i]
		})
	}
	for _, featureView := range odfvs {
		rand.Shuffle(len(featureView.Base.Features), func(i, j int) {
			featureView.Base.Features[i], featureView.Base.Features[j] = featureView.Base.Features[j], featureView.Base.Features[i]
		})
	}

	schema, err := GetSchemaFromFeatureService(featureService, entityMap, featureViews, odfvs)
	assert.Nil(t, err)

	// Ensure the same results
	assert.Equal(t, expectedFeatureNames, schema.Features)
	assert.Equal(t, expectedEntityNames, schema.Entities)
	for _, entityName := range expectedEntityNames {
		assert.Contains(t, schema.EntityTypes, entityName)
	}
	assert.True(t, reflect.DeepEqual(schema.EntityTypes["driver_id"], types.ValueType_INT64))

	types := []types.ValueType_Enum{*types.ValueType_INT64.Enum(), *types.ValueType_FLOAT.Enum(), *types.ValueType_INT32.Enum(), *types.ValueType_DOUBLE.Enum(), *types.ValueType_INT32.Enum(), *types.ValueType_DOUBLE.Enum()}
	for idx, featureName := range expectedFeatureNames {
		assert.Contains(t, schema.FeaturesTypes, featureName)
		assert.Equal(t, schema.FeaturesTypes[featureName], types[idx])
	}
}

func TestSerializeToArrowTable(t *testing.T) {
	table, expectedSchema, expectedColumns, err := GetTestArrowTableAndExpectedResults()
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
			if name == "RequestId" {
				continue
			}
			assert.Equal(t, len(val.Val), len(expectedColumns[name].Val))
			for idx, featureVal := range val.Val {
				assert.Equal(t, featureVal.Val, expectedColumns[name].Val[idx].Val)
			}
		}
	}
}

// Initialize all dummy featureservice, entities and featureviews/on demand featureviews for testing.
func InitializeFeatureRepoVariablesForTest() (*model.FeatureService, []*model.Entity, []*model.FeatureView, []*model.OnDemandFeatureView) {
	f1 := test.CreateNewFeature(
		"int64",
		types.ValueType_INT64,
	)
	f2 := test.CreateNewFeature(
		"float32",
		types.ValueType_FLOAT,
	)
	projection1 := test.CreateNewFeatureViewProjection(
		"featureView1",
		"",
		[]*model.Feature{f1, f2},
		map[string]string{},
	)
	baseFeatureView1 := test.CreateBaseFeatureView(
		"featureView1",
		[]*model.Feature{f1, f2},
		projection1,
	)
	featureView1 := test.CreateFeatureView(baseFeatureView1, nil, []string{"driver_id"}, map[string]struct{}{"driver_id": {}})
	entity1 := test.CreateNewEntity("driver_id", types.ValueType_INT64, "driver_id")
	f3 := test.CreateNewFeature(
		"int32",
		types.ValueType_INT32,
	)
	f4 := test.CreateNewFeature(
		"double",
		types.ValueType_DOUBLE,
	)
	projection2 := test.CreateNewFeatureViewProjection(
		"featureView2",
		"",
		[]*model.Feature{f3, f4},
		map[string]string{},
	)
	baseFeatureView2 := test.CreateBaseFeatureView(
		"featureView2",
		[]*model.Feature{f3, f4},
		projection2,
	)
	featureView2 := test.CreateFeatureView(baseFeatureView2, nil, []string{"driver_id"}, map[string]struct{}{"driver_id": {}})

	f5 := test.CreateNewFeature(
		"odfv_f1",
		types.ValueType_INT32,
	)
	f6 := test.CreateNewFeature(
		"odfv_f2",
		types.ValueType_DOUBLE,
	)
	projection3 := test.CreateNewFeatureViewProjection(
		"od_bf1",
		"",
		[]*model.Feature{f5, f6},
		map[string]string{},
	)
	od_bf1 := test.CreateBaseFeatureView(
		"od_bf1",
		[]*model.Feature{f5, f6},
		projection3,
	)
	odfv := model.NewOnDemandFeatureViewFromBase(od_bf1)
	featureService := test.CreateNewFeatureService(
		"test_service",
		"test_project",
		nil,
		nil,
		[]*model.FeatureViewProjection{projection1, projection2, projection3},
	)
	return featureService, []*model.Entity{entity1}, []*model.FeatureView{featureView1, featureView2}, []*model.OnDemandFeatureView{odfv}
}

// Create dummy FeatureService, Entities, and FeatureViews add them to the logger and convert the logs to Arrow table.
// Returns arrow table, expected test schema, and expected columns.
func GetTestArrowTableAndExpectedResults() (array.Table, map[string]arrow.DataType, map[string]*types.RepeatedValue, error) {
	featureService, entities, featureViews, odfvs := InitializeFeatureRepoVariablesForTest()
	entityMap := make(map[string]*model.Entity)
	for _, entity := range entities {
		entityMap[entity.Name] = entity
	}
	schema, err := GetSchemaFromFeatureService(featureService, entityMap, featureViews, odfvs)
	if err != nil {
		return nil, nil, nil, err
	}

	ts := timestamppb.New(time.Now())
	log1 := Log{
		EntityValue: []*types.Value{
			{Val: &types.Value_Int64Val{Int64Val: 1001}},
		},
		FeatureValues: []*types.Value{
			/* normal feature values */
			{Val: &types.Value_Int64Val{Int64Val: rand.Int63()}},
			{Val: &types.Value_FloatVal{FloatVal: rand.Float32()}},
			{Val: &types.Value_Int32Val{Int32Val: rand.Int31()}},
			{Val: &types.Value_DoubleVal{DoubleVal: rand.Float64()}},
			/* odfv values */
			{Val: &types.Value_Int32Val{Int32Val: rand.Int31()}},
			{Val: &types.Value_DoubleVal{DoubleVal: rand.Float64()}},
		},
		FeatureStatuses: []serving.FieldStatus{
			serving.FieldStatus_PRESENT,
			serving.FieldStatus_PRESENT,
			serving.FieldStatus_PRESENT,
			serving.FieldStatus_PRESENT,
			serving.FieldStatus_PRESENT,
			serving.FieldStatus_PRESENT,
		},
		EventTimestamps: []*timestamppb.Timestamp{
			ts, ts, ts, ts, ts, ts,
		},
	}
	log2 := Log{
		EntityValue: []*types.Value{
			{Val: &types.Value_Int64Val{Int64Val: 1003}},
		},
		FeatureValues: []*types.Value{
			/* normal feature values */
			{Val: &types.Value_Int64Val{Int64Val: rand.Int63()}},
			{Val: &types.Value_FloatVal{FloatVal: rand.Float32()}},
			{Val: &types.Value_Int32Val{Int32Val: rand.Int31()}},
			{Val: &types.Value_DoubleVal{DoubleVal: rand.Float64()}},
			/* odfv values */
			{Val: &types.Value_Int32Val{Int32Val: rand.Int31()}},
			{Val: &types.Value_DoubleVal{DoubleVal: rand.Float64()}},
		},
		FeatureStatuses: []serving.FieldStatus{
			serving.FieldStatus_PRESENT,
			serving.FieldStatus_PRESENT,
			serving.FieldStatus_PRESENT,
			serving.FieldStatus_PRESENT,
			serving.FieldStatus_PRESENT,
			serving.FieldStatus_PRESENT,
		},
		EventTimestamps: []*timestamppb.Timestamp{
			ts, ts, ts, ts, ts, ts,
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
	expectedSchema["RequestId"] = arrow.BinaryTypes.String
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
		"featureView1__int64": {
			Val: []*types.Value{
				log1.FeatureValues[0],
				log2.FeatureValues[0]},
		},
		"featureView1__float32": {
			Val: []*types.Value{
				log1.FeatureValues[1],
				log2.FeatureValues[1]},
		},
		"featureView2__int32": {
			Val: []*types.Value{
				log1.FeatureValues[2],
				log2.FeatureValues[2]},
		},
		"featureView2__double": {
			Val: []*types.Value{
				log1.FeatureValues[3],
				log2.FeatureValues[3]},
		},
		"od_bf1__odfv_f1": {
			Val: []*types.Value{
				log1.FeatureValues[4],
				log2.FeatureValues[4]},
		},
		"od_bf1__odfv_f2": {
			Val: []*types.Value{
				log1.FeatureValues[5],
				log2.FeatureValues[5]},
		},
	}
	loggingService, err := SetupLoggingServiceWithLogs([]*Log{&log1, &log2})
	if err != nil {
		return nil, nil, nil, err
	}

	table, err := ConvertMemoryBufferToArrowTable(loggingService.memoryBuffer, schema)

	if err != nil {
		return nil, nil, nil, err
	}
	return table, expectedSchema, expectedColumns, nil
}

func SetupLoggingServiceWithLogs(logs []*Log) (*LoggingService, error) {
	loggingService, err := NewLoggingService(nil, len(logs), "", false)
	if err != nil {
		return nil, err
	}
	dummyTicker := time.NewTicker(10 * time.Second)
	// stop the ticker so that the logs are not flushed to offline storage
	dummyTicker.Stop()
	for _, log := range logs {
		loggingService.EmitLog(log)
	}
	// manually handle flushing logs
	for i := 0; i < len(logs); i++ {
		loggingService.PerformPeriodicAppendToMemoryBufferAndLogFlush(dummyTicker)
	}
	return loggingService, nil
}
