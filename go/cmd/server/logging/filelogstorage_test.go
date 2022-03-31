package logging

import (
	"log"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/feast-dev/feast/go/protos/feast/serving"
	"github.com/feast-dev/feast/go/protos/feast/types"
	"github.com/stretchr/testify/assert"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/reader"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestWriteToLogStorage(t *testing.T) {
	offlineStoreConfig := map[string]interface{}{
		"path": "log.parquet",
	}
	fileStore, err := NewFileOfflineStore("test", offlineStoreConfig)
	assert.Nil(t, err)
	ts := timestamppb.New(time.Now())
	newLog := Log{
		EntityName:      "driver_id",
		EntityValue:     &types.Value{Val: &types.Value_Int64Val{Int64Val: 1001}},
		FeatureNames:    []string{"conv_rate", "acc_rate"},
		FeatureValues:   []*types.Value{{Val: &types.Value_FloatVal{FloatVal: 0.2}}, {Val: &types.Value_FloatVal{FloatVal: 0.5}}},
		FeatureStatuses: []serving.FieldStatus{serving.FieldStatus_PRESENT, serving.FieldStatus_PRESENT},
		EventTimestamps: []*timestamppb.Timestamp{ts, ts},
	}
	newLog2 := Log{
		EntityName:      "driver_id",
		EntityValue:     &types.Value{Val: &types.Value_Int64Val{Int64Val: 1003}},
		FeatureNames:    []string{"feature4", "feature5"},
		FeatureValues:   []*types.Value{{Val: &types.Value_FloatVal{FloatVal: 0.3}}, {Val: &types.Value_FloatVal{FloatVal: 0.8}}},
		FeatureStatuses: []serving.FieldStatus{serving.FieldStatus_PRESENT, serving.FieldStatus_PRESENT},
		EventTimestamps: []*timestamppb.Timestamp{ts, ts},
	}
	memoryBuffer := MemoryBuffer{
		logs: []*Log{&newLog, &newLog2},
	}

	err = fileStore.FlushToStorage(&memoryBuffer)
	assert.Nil(t, err)

	///read
	fr, err := local.NewLocalFileReader("log.parquet")
	assert.Nil(t, err)

	pr, err := reader.NewParquetReader(fr, new(ParquetLog), 4)
	if err != nil {
		log.Println("Can't create parquet reader", err)
		return
	}
	num := int(pr.GetNumRows())
	assert.Equal(t, num, 2)
	logs := make([]ParquetLog, 2) //read 10 rows
	if err = pr.Read(&logs); err != nil {
		log.Println("Read error", err)
	}

	for i := 0; i < 2; i++ {
		assert.Equal(t, logs[i].EntityName, memoryBuffer.logs[i].EntityName)
		assert.Equal(t, logs[i].EntityValue, memoryBuffer.logs[i].EntityValue.String())
		assert.True(t, reflect.DeepEqual(logs[i].FeatureNames, memoryBuffer.logs[i].FeatureNames))
		numValues := len(memoryBuffer.logs[i].FeatureValues)
		assert.Equal(t, numValues, len(logs[i].FeatureValues))
		assert.Equal(t, numValues, len(logs[i].EventTimestamps))
		assert.Equal(t, numValues, len(logs[i].FeatureStatuses))
		for idx := 0; idx < numValues; idx++ {
			assert.Equal(t, logs[i].EventTimestamps[idx], memoryBuffer.logs[i].EventTimestamps[idx].AsTime().UnixNano()/int64(time.Millisecond))
			if memoryBuffer.logs[i].FeatureStatuses[idx] == serving.FieldStatus_PRESENT {
				assert.True(t, logs[i].FeatureStatuses[idx])
			} else {
				assert.False(t, logs[i].FeatureStatuses[idx])
			}
			assert.Equal(t, logs[i].FeatureValues[idx], memoryBuffer.logs[i].FeatureValues[idx].String())
		}
	}

	pr.ReadStop()
	fr.Close()

	err = os.Remove("log.parquet")
	assert.Nil(t, err)
}
