package logging

import (
	"path/filepath"

	"context"
	"os"
	"reflect"
	"testing"

	"github.com/apache/arrow/go/v8/arrow"
	"github.com/apache/arrow/go/v8/arrow/array"
	"github.com/apache/arrow/go/v8/arrow/memory"
	"github.com/apache/arrow/go/v8/parquet/file"
	"github.com/apache/arrow/go/v8/parquet/pqarrow"
	"github.com/feast-dev/feast/go/protos/feast/types"
	"github.com/stretchr/testify/assert"
)

func TestFlushToStorage(t *testing.T) {
	table, err := GenerateLogsAndConvertToArrowTable()
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
