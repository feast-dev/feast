package logging

import (
	"path/filepath"

	"context"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/apache/arrow/go/v8/arrow"
	"github.com/apache/arrow/go/v8/arrow/array"
	"github.com/apache/arrow/go/v8/arrow/memory"
	"github.com/apache/arrow/go/v8/parquet/file"
	"github.com/apache/arrow/go/v8/parquet/pqarrow"
	"github.com/feast-dev/feast/go/protos/feast/serving"
	"github.com/feast-dev/feast/go/protos/feast/types"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// func TestWriteToLogStorage(t *testing.T) {
// 	offlineStoreConfig := map[string]interface{}{
// 		"path": ".",
// 	}
// 	fileStore, err := NewFileOfflineStore("test", offlineStoreConfig)
// 	assert.Nil(t, err)
// 	ts := timestamppb.New(time.Now())
// 	newLog := Log{
// 		EntityName:      "driver_id",
// 		EntityValue:     &types.Value{Val: &types.Value_Int64Val{Int64Val: 1001}},
// 		FeatureNames:    []string{"conv_rate", "acc_rate"},
// 		FeatureValues:   []*types.Value{{Val: &types.Value_FloatVal{FloatVal: 0.2}}, {Val: &types.Value_FloatVal{FloatVal: 0.5}}},
// 		FeatureStatuses: []serving.FieldStatus{serving.FieldStatus_PRESENT, serving.FieldStatus_PRESENT},
// 		EventTimestamps: []*timestamppb.Timestamp{ts, ts},
// 	}
// 	newLog2 := Log{
// 		EntityName:      "driver_id",
// 		EntityValue:     &types.Value{Val: &types.Value_Int64Val{Int64Val: 1003}},
// 		FeatureNames:    []string{"feature4", "feature5"},
// 		FeatureValues:   []*types.Value{{Val: &types.Value_FloatVal{FloatVal: 0.3}}, {Val: &types.Value_FloatVal{FloatVal: 0.8}}},
// 		FeatureStatuses: []serving.FieldStatus{serving.FieldStatus_PRESENT, serving.FieldStatus_PRESENT},
// 		EventTimestamps: []*timestamppb.Timestamp{ts, ts},
// 	}
// 	memoryBuffer := MemoryBuffer{
// 		logs: []*Log{&newLog, &newLog2},
// 	}

// 	err = fileStore.FlushToStorage(&memoryBuffer)
// 	assert.Nil(t, err)

// 	///read
// 	fr, err := local.NewLocalFileReader("log.parquet")
// 	assert.Nil(t, err)

// 	pr, err := reader.NewParquetReader(fr, new(ParquetLog), 4)
// 	if err != nil {
// 		log.Println("Can't create parquet reader", err)
// 		return
// 	}
// 	num := int(pr.GetNumRows())
// 	assert.Equal(t, num, 2)
// 	logs := make([]ParquetLog, 2) //read 10 rows
// 	if err = pr.Read(&logs); err != nil {
// 		log.Println("Read error", err)
// 	}

// 	for i := 0; i < 2; i++ {
// 		assert.Equal(t, logs[i].EntityName, memoryBuffer.logs[i].EntityName)
// 		assert.Equal(t, logs[i].EntityValue, memoryBuffer.logs[i].EntityValue.String())
// 		assert.True(t, reflect.DeepEqual(logs[i].FeatureNames, memoryBuffer.logs[i].FeatureNames))
// 		numValues := len(memoryBuffer.logs[i].FeatureValues)
// 		assert.Equal(t, numValues, len(logs[i].FeatureValues))
// 		assert.Equal(t, numValues, len(logs[i].EventTimestamps))
// 		assert.Equal(t, numValues, len(logs[i].FeatureStatuses))
// 		for idx := 0; idx < numValues; idx++ {
// 			assert.Equal(t, logs[i].EventTimestamps[idx], memoryBuffer.logs[i].EventTimestamps[idx].AsTime().UnixNano()/int64(time.Millisecond))
// 			if memoryBuffer.logs[i].FeatureStatuses[idx] == serving.FieldStatus_PRESENT {
// 				assert.True(t, logs[i].FeatureStatuses[idx])
// 			} else {
// 				assert.False(t, logs[i].FeatureStatuses[idx])
// 			}
// 			assert.Equal(t, logs[i].FeatureValues[idx], memoryBuffer.logs[i].FeatureValues[idx].String())
// 		}
// 	}

// 	pr.ReadStop()
// 	fr.Close()

// 	err = os.Remove("log.parquet")
// 	assert.Nil(t, err)
// }

func TestFlushToStorage(t *testing.T) {
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
	table, err := loggingService.GetLogInArrowTable(schema)
	defer table.Release()
	assert.Nil(t, err)
	offlineStoreConfig := map[string]interface{}{
		"path": ".",
	}
	fileStore, err := NewFileOfflineStore("test", offlineStoreConfig)
	assert.Nil(t, err)
	err = fileStore.FlushToStorage(array.Table(table))
	assert.Nil(t, err)
	logPath, err := filepath.Abs(filepath.Join(".", "log.parquet"))
	assert.Nil(t, err)
	w, err := CreateOrOpenLogFile(logPath)
	assert.Nil(t, err)
	ctx := context.Background()

	pf, err := file.NewParquetReader(w)
	assert.Nil(t, err)

	reader, err := pqarrow.NewFileReader(pf, pqarrow.ArrowReadProperties{}, memory.DefaultAllocator)
	assert.Nil(t, err)

	tbl, err := reader.ReadTable(ctx)
	assert.Nil(t, err)
	tr := array.NewTableReader(tbl, -1)
	defer tbl.Release()
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

	err = CleanUpTestLogs(logPath)
	assert.Nil(t, err)

}

func CleanUpTestLogs(absPath string) error {
	return os.Remove(absPath)
}
