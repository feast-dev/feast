package logging

import (
	"context"
	"io/ioutil"
	"path/filepath"
	"testing"
	"time"

	"github.com/apache/arrow/go/v8/arrow"
	"github.com/apache/arrow/go/v8/arrow/array"
	"github.com/apache/arrow/go/v8/arrow/memory"
	"github.com/apache/arrow/go/v8/parquet/file"
	"github.com/apache/arrow/go/v8/parquet/pqarrow"
	"github.com/stretchr/testify/require"

	"github.com/feast-dev/feast/go/protos/feast/types"

	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/feast-dev/feast/go/protos/feast/serving"
)

type DummySink struct{}

func (s *DummySink) Write(recs []arrow.Record) error {
	return nil
}

func (s *DummySink) Flush(featureServiceName string) error {
	return nil
}

func TestLoggingChannelTimeout(t *testing.T) {
	config := LoggerConfig{
		SampleRate: 1.0,
		LoggingOptions: LoggingOptions{
			ChannelCapacity: 1,
			EmitTimeout:     DefaultOptions.EmitTimeout,
			FlushInterval:   DefaultOptions.FlushInterval,
			WriteInterval:   DefaultOptions.WriteInterval,
		},
	}
	logger, err := NewLogger(&FeatureServiceSchema{}, "testFS", &DummySink{}, config)

	// stop log processing to check buffering channel
	logger.Stop()
	logger.WaitUntilStopped()

	assert.Nil(t, err)
	assert.Empty(t, logger.buffer.logs)
	ts := timestamppb.New(time.Now())
	newLog := Log{
		FeatureStatuses: []serving.FieldStatus{serving.FieldStatus_PRESENT},
		EventTimestamps: []*timestamppb.Timestamp{ts, ts},
	}
	err = logger.EmitLog(&newLog)
	assert.Nil(t, err)

	newLog2 := Log{
		FeatureStatuses: []serving.FieldStatus{serving.FieldStatus_PRESENT},
		EventTimestamps: []*timestamppb.Timestamp{ts, ts},
	}
	err = logger.EmitLog(&newLog2)
	// The channel times out and doesn't hang.
	assert.NotNil(t, err)
}

func TestLogAndFlushToFile(t *testing.T) {
	sink, err := NewFileLogSink(t.TempDir())
	assert.Nil(t, err)

	schema := &FeatureServiceSchema{
		JoinKeys:      []string{"driver_id"},
		Features:      []string{"view__feature"},
		JoinKeysTypes: map[string]types.ValueType_Enum{"driver_id": types.ValueType_INT32},
		FeaturesTypes: map[string]types.ValueType_Enum{"view__feature": types.ValueType_DOUBLE},
	}
	config := LoggerConfig{
		SampleRate: 1.0,
		LoggingOptions: LoggingOptions{
			ChannelCapacity: DefaultOptions.ChannelCapacity,
			EmitTimeout:     DefaultOptions.EmitTimeout,
			FlushInterval:   DefaultOptions.FlushInterval,
			WriteInterval:   10 * time.Millisecond,
		},
	}
	logger, err := NewLogger(schema, "testFS", sink, config)
	assert.Nil(t, err)

	assert.Nil(t, logger.Log(
		map[string]*types.RepeatedValue{
			"driver_id": {
				Val: []*types.Value{
					{
						Val: &types.Value_Int32Val{
							Int32Val: 111,
						},
					},
				},
			},
		},
		[]*serving.GetOnlineFeaturesResponse_FeatureVector{
			{
				Values:          []*types.Value{{Val: &types.Value_DoubleVal{DoubleVal: 2.0}}},
				Statuses:        []serving.FieldStatus{serving.FieldStatus_PRESENT},
				EventTimestamps: []*timestamppb.Timestamp{timestamppb.Now()},
			},
		},
		[]string{"view__feature"},
		map[string]*types.RepeatedValue{},
		"req-id",
	))

	require.Eventually(t, func() bool {
		files, _ := ioutil.ReadDir(sink.path)
		return len(files) > 0
	}, 60*time.Second, 100*time.Millisecond)

	files, _ := ioutil.ReadDir(sink.path)

	pf, err := file.OpenParquetFile(filepath.Join(sink.path, files[0].Name()), false)
	assert.Nil(t, err)

	reader, err := pqarrow.NewFileReader(pf, pqarrow.ArrowReadProperties{}, memory.DefaultAllocator)
	assert.Nil(t, err)

	tbl, err := reader.ReadTable(context.Background())
	assert.Nil(t, err)
	tr := array.NewTableReader(tbl, -1)
	defer tbl.Release()

	fieldNameToIdx := make(map[string]int)
	for idx, field := range tbl.Schema().Fields() {
		fieldNameToIdx[field.Name] = idx
	}

	tr.Next()
	rec := tr.Record()

	assert.Equal(t, "req-id", rec.Column(fieldNameToIdx[LOG_REQUEST_ID_FIELD]).(*array.String).Value(0))
	assert.EqualValues(t, 111, rec.Column(fieldNameToIdx["driver_id"]).(*array.Int32).Value(0))
	assert.EqualValues(t, 2.0, rec.Column(fieldNameToIdx["view__feature"]).(*array.Float64).Value(0))
	assert.EqualValues(t, serving.FieldStatus_PRESENT, rec.Column(fieldNameToIdx["view__feature__status"]).(*array.Int32).Value(0))

}
