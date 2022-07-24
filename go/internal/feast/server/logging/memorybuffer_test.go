package logging

import (
	"math/rand"
	"testing"
	"time"

	"github.com/apache/arrow/go/v8/arrow"
	"github.com/apache/arrow/go/v8/arrow/array"
	"github.com/apache/arrow/go/v8/arrow/memory"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/feast-dev/feast/go/protos/feast/serving"
	"github.com/feast-dev/feast/go/protos/feast/types"
)

func TestArrowSchemaGeneration(t *testing.T) {
	schema := &FeatureServiceSchema{
		JoinKeys: []string{
			"driver_id",
		},
		Features: []string{
			"featureView1__int64",
			"featureView1__float32",
			"featureView2__int32",
			"featureView2__double",
		},
		JoinKeysTypes: map[string]types.ValueType_Enum{
			"driver_id": types.ValueType_INT32,
		},
		FeaturesTypes: map[string]types.ValueType_Enum{
			"featureView1__int64":   types.ValueType_INT64,
			"featureView1__float32": types.ValueType_FLOAT,
			"featureView2__int32":   types.ValueType_INT32,
			"featureView2__double":  types.ValueType_DOUBLE,
		},
	}

	expectedArrowSchema := []arrow.Field{
		{Name: "driver_id", Type: arrow.PrimitiveTypes.Int32},
		{Name: "featureView1__int64", Type: arrow.PrimitiveTypes.Int64},
		{Name: "featureView1__int64__timestamp", Type: arrow.FixedWidthTypes.Timestamp_s},
		{Name: "featureView1__int64__status", Type: arrow.PrimitiveTypes.Int32},
		{Name: "featureView1__float32", Type: arrow.PrimitiveTypes.Float32},
		{Name: "featureView1__float32__timestamp", Type: arrow.FixedWidthTypes.Timestamp_s},
		{Name: "featureView1__float32__status", Type: arrow.PrimitiveTypes.Int32},
		{Name: "featureView2__int32", Type: arrow.PrimitiveTypes.Int32},
		{Name: "featureView2__int32__timestamp", Type: arrow.FixedWidthTypes.Timestamp_s},
		{Name: "featureView2__int32__status", Type: arrow.PrimitiveTypes.Int32},
		{Name: "featureView2__double", Type: arrow.PrimitiveTypes.Float64},
		{Name: "featureView2__double__timestamp", Type: arrow.FixedWidthTypes.Timestamp_s},
		{Name: "featureView2__double__status", Type: arrow.PrimitiveTypes.Int32},
		{Name: "__log_timestamp", Type: arrow.FixedWidthTypes.Timestamp_us},
		{Name: "__log_date", Type: arrow.FixedWidthTypes.Date32},
		{Name: "__request_id", Type: arrow.BinaryTypes.String},
	}

	actualSchema, err := getArrowSchema(schema)
	assert.Nil(t, err)
	assert.Equal(t, expectedArrowSchema, actualSchema.Fields())
}

func TestSerializeToArrowTable(t *testing.T) {
	schema := &FeatureServiceSchema{
		JoinKeys: []string{
			"driver_id",
		},
		Features: []string{
			"featureView1__int64",
			"featureView1__float32",
		},
		JoinKeysTypes: map[string]types.ValueType_Enum{
			"driver_id": types.ValueType_INT32,
		},
		FeaturesTypes: map[string]types.ValueType_Enum{
			"featureView1__int64":   types.ValueType_INT64,
			"featureView1__float32": types.ValueType_FLOAT,
		},
	}

	ts := timestamppb.New(time.Now())
	b, _ := NewMemoryBuffer(schema)
	b.Append(&Log{
		EntityValue: []*types.Value{
			{Val: &types.Value_Int64Val{Int64Val: 1001}},
		},
		FeatureValues: []*types.Value{
			{Val: &types.Value_Int64Val{Int64Val: rand.Int63()}},
			{Val: &types.Value_FloatVal{FloatVal: rand.Float32()}},
		},
		FeatureStatuses: []serving.FieldStatus{
			serving.FieldStatus_PRESENT,
			serving.FieldStatus_OUTSIDE_MAX_AGE,
		},
		EventTimestamps: []*timestamppb.Timestamp{
			ts, ts,
		},
		RequestId:    "aaa",
		LogTimestamp: time.Now(),
	})
	b.Append(&Log{
		EntityValue: []*types.Value{
			{Val: &types.Value_Int64Val{Int64Val: 1003}},
		},
		FeatureValues: []*types.Value{
			{Val: &types.Value_Int64Val{Int64Val: rand.Int63()}},
			{Val: &types.Value_FloatVal{FloatVal: rand.Float32()}},
		},
		FeatureStatuses: []serving.FieldStatus{
			serving.FieldStatus_PRESENT,
			serving.FieldStatus_PRESENT,
		},
		EventTimestamps: []*timestamppb.Timestamp{
			ts, ts,
		},
		RequestId:    "bbb",
		LogTimestamp: time.Now(),
	})

	pool := memory.NewCgoArrowAllocator()
	builder := array.NewRecordBuilder(pool, b.arrowSchema)
	defer builder.Release()

	// join key: driver_id
	builder.Field(0).(*array.Int32Builder).AppendValues(
		[]int32{b.logs[0].EntityValue[0].GetInt32Val(), b.logs[1].EntityValue[0].GetInt32Val()}, []bool{true, true})

	// feature int64
	builder.Field(1).(*array.Int64Builder).AppendValues(
		[]int64{b.logs[0].FeatureValues[0].GetInt64Val(), b.logs[1].FeatureValues[0].GetInt64Val()}, []bool{true, true})
	builder.Field(2).(*array.TimestampBuilder).AppendValues(
		[]arrow.Timestamp{arrow.Timestamp(ts.GetSeconds()), arrow.Timestamp(ts.GetSeconds())}, []bool{true, true})
	builder.Field(3).(*array.Int32Builder).AppendValues(
		[]int32{int32(serving.FieldStatus_PRESENT), int32(serving.FieldStatus_PRESENT)}, []bool{true, true})

	// feature float
	builder.Field(4).(*array.Float32Builder).AppendValues(
		[]float32{b.logs[0].FeatureValues[1].GetFloatVal(), b.logs[1].FeatureValues[1].GetFloatVal()}, []bool{true, true})
	builder.Field(5).(*array.TimestampBuilder).AppendValues(
		[]arrow.Timestamp{arrow.Timestamp(ts.GetSeconds()), arrow.Timestamp(ts.GetSeconds())}, []bool{true, true})
	builder.Field(6).(*array.Int32Builder).AppendValues(
		[]int32{int32(serving.FieldStatus_OUTSIDE_MAX_AGE), int32(serving.FieldStatus_PRESENT)}, []bool{true, true})

	// log timestamp
	builder.Field(7).(*array.TimestampBuilder).AppendValues(
		[]arrow.Timestamp{arrow.Timestamp(b.logs[0].LogTimestamp.UnixMicro()), arrow.Timestamp(b.logs[1].LogTimestamp.UnixMicro())}, []bool{true, true})

	// log date
	today := time.Now().Truncate(24 * time.Hour)
	builder.Field(8).(*array.Date32Builder).AppendValues(
		[]arrow.Date32{arrow.Date32FromTime(today), arrow.Date32FromTime(today)}, []bool{true, true})

	// request id
	builder.Field(9).(*array.StringBuilder).AppendValues(
		[]string{b.logs[0].RequestId, b.logs[1].RequestId}, []bool{true, true})

	record, err := b.convertToArrowRecord()
	expectedRecord := builder.NewRecord()
	assert.Nil(t, err)
	for colIdx := 0; colIdx < int(record.NumCols()); colIdx++ {
		assert.True(t, array.Equal(expectedRecord.Column(colIdx), record.Column(colIdx)), "Columns with idx %d are not equal", colIdx)
	}

}
