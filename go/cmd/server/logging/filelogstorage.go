package logging

import (
	"errors"
	"io"
	"os"
	"path/filepath"

	"github.com/apache/arrow/go/v8/arrow/array"
	"github.com/apache/arrow/go/v8/parquet"
	"github.com/apache/arrow/go/v8/parquet/pqarrow"
)

type FileLogStorage struct {
	// Feast project name
	project string
	path    string
}

type ParquetLog struct {
	EntityName      string   `parquet:"name=entityname, type=BYTE_ARRAY"`
	EntityValue     string   `parquet:"name=entityvalue, type=BYTE_ARRAY"`
	FeatureNames    []string `parquet:"name=featurenames, type=MAP, convertedtype=LIST, valuetype=BYTE_ARRAY, valueconvertedtype=UTF8"`
	FeatureValues   []string `parquet:"name=featurevalues, type=MAP, convertedtype=LIST, valuetype=BYTE_ARRAY, valueconvertedtype=UTF8"`
	FeatureStatuses []bool   `parquet:"name=featurestatuses, type=MAP, convertedtype=LIST, valuetype=BOOLEAN"`
	EventTimestamps []int64  `parquet:"name=eventtimestamps, type=MAP, convertedtype=LIST, valuetype=INT64, valueconvertedtype=TIMESTAMP_MILLIS"`
}

func NewFileOfflineStore(project string, offlineStoreConfig map[string]interface{}) (*FileLogStorage, error) {
	store := FileLogStorage{project: project}
	var abs_path string
	var err error
	if val, ok := offlineStoreConfig["path"]; !ok {
		abs_path, err = filepath.Abs("log.parquet")
	} else {
		result, ok := val.(string)
		if !ok {
			return nil, errors.New("cannot convert offlinestore path to string")
		}
		abs_path, err = filepath.Abs(filepath.Join(result, "log.parquet"))
	}
	if err != nil {
		return nil, err
	}
	store.path = abs_path
	return &store, nil
}

func CreateOrOpenLogFile(absPath string) (*os.File, error) {
	var _, err = os.Stat(absPath)

	// create file if not exists
	if os.IsNotExist(err) {
		var file, err = os.Create(absPath)
		if err != nil {
			return nil, err
		}
		return file, nil
	} else {
		var file, err = os.OpenFile(absPath, os.O_RDWR, 0644)
		if err != nil {
			return nil, err
		}
		return file, nil
	}
}

func (f *FileLogStorage) FlushToStorage(tbl array.Table) error {
	w, err := CreateOrOpenLogFile(f.path)
	var writer io.Writer = w
	if err != nil {
		return err
	}
	props := parquet.NewWriterProperties(parquet.WithDictionaryDefault(false))
	arrProps := pqarrow.DefaultWriterProps()
	err = pqarrow.WriteTable(tbl, writer, 100, props, arrProps)
	if err != nil {
		return err
	}
	return nil

}
