package logging

import (
	"log"
	"os"
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
		FeatureNames:    []string{"feature1", "feature2"},
		FeatureValues:   []*types.Value{{Val: &types.Value_Int64Val{Int64Val: 1001}}, {Val: &types.Value_Int64Val{Int64Val: 1002}}},
		FeatureStatuses: []serving.FieldStatus{serving.FieldStatus_PRESENT},
		EventTimestamps: []*timestamppb.Timestamp{ts, ts},
	}
	newLog2 := Log{
		FeatureNames:    []string{"feature4", "feature5"},
		FeatureValues:   []*types.Value{{Val: &types.Value_Int64Val{Int64Val: 1003}}, {Val: &types.Value_Int64Val{Int64Val: 1004}}},
		FeatureStatuses: []serving.FieldStatus{serving.FieldStatus_PRESENT},
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
	log.Println(logs)

	pr.ReadStop()
	fr.Close()

	err = os.Remove("log.parquet")
	assert.Nil(t, err)
}
