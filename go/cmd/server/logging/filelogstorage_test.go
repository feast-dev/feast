package logging

import (
	"path/filepath"

	"context"
	"testing"

	"github.com/apache/arrow/go/v8/arrow/array"
	"github.com/apache/arrow/go/v8/arrow/memory"
	"github.com/apache/arrow/go/v8/parquet/file"
	"github.com/apache/arrow/go/v8/parquet/pqarrow"
	"github.com/feast-dev/feast/go/internal/test"
	"github.com/stretchr/testify/assert"
)

func TestFlushToStorage(t *testing.T) {
	table, expectedSchema, expectedColumns, err := GenerateLogsAndConvertToArrowTable()
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

	err = test.CleanUpLogs(logPath)
	assert.Nil(t, err)
}
