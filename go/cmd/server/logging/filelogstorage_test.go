package logging

import (
	"context"
	"path/filepath"

	"testing"

	"github.com/apache/arrow/go/v8/arrow/array"
	"github.com/apache/arrow/go/v8/arrow/memory"
	"github.com/apache/arrow/go/v8/parquet/file"
	"github.com/apache/arrow/go/v8/parquet/pqarrow"
	"github.com/feast-dev/feast/go/internal/test"
	"github.com/stretchr/testify/assert"
)

func TestFlushToStorage(t *testing.T) {
	ctx := context.Background()
	table, expectedSchema, expectedColumns, err := GetTestArrowTableAndExpectedResults()
	defer table.Release()
	assert.Nil(t, err)
	offlineStoreConfig := OfflineLogStoreConfig{
		storeType: "file",
		path:      "./log.parquet",
	}
	fileStore, err := NewFileOfflineStore("test", &offlineStoreConfig)
	assert.Nil(t, err)
	err = fileStore.FlushToStorage(array.Table(table))
	assert.Nil(t, err)
	logPath, err := filepath.Abs(offlineStoreConfig.path)
	assert.Nil(t, err)
	pf, err := file.OpenParquetFile(logPath, false)
	assert.Nil(t, err)

	reader, err := pqarrow.NewFileReader(pf, pqarrow.ArrowReadProperties{}, memory.DefaultAllocator)
	assert.Nil(t, err)

	tbl, err := reader.ReadTable(ctx)
	assert.Nil(t, err)
	tr := array.NewTableReader(tbl, -1)
	defer tbl.Release()

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
				// Ensure there are request ids in record.
				assert.Greater(t, len(val.Val), 0)
			} else {
				assert.Equal(t, len(val.Val), len(expectedColumns[name].Val))
				for idx, featureVal := range val.Val {
					assert.Equal(t, featureVal.Val, expectedColumns[name].Val[idx].Val)
				}
			}
		}
	}

	err = test.CleanUpFiles(logPath)
	assert.Nil(t, err)
}
