package logging

import (
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/feast-dev/feast/go/protos/feast/serving"
	"github.com/xitongsys/parquet-go/writer"
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

func (f *FileLogStorage) FlushToStorage(m *MemoryBuffer) error {
	if len(m.logs) == 0 {
		return nil
	}
	var err error
	w, err := CreateOrOpenLogFile(f.path)
	if err != nil {
		return fmt.Errorf("can't create local file with error: %s", err)
	}
	pw, err := writer.NewParquetWriterFromWriter(w, new(ParquetLog), 4)
	if err != nil {
		return fmt.Errorf("can't create parquet writer with error: %s", err)
	}

	for _, newLog := range m.logs {
		numValues := len(newLog.FeatureValues)
		if numValues != len(newLog.FeatureStatuses) || numValues != len(newLog.EventTimestamps) {
			return errors.New("length of log arrays do not match")
		}
		statuses := make([]bool, numValues)
		timestampsInMillis := make([]int64, numValues)
		featureValues := make([]string, numValues)
		for idx := 0; idx < numValues; idx++ {
			if newLog.FeatureStatuses[idx] == serving.FieldStatus_PRESENT {
				statuses[idx] = true
			} else {
				statuses[idx] = false
			}
			ts := newLog.EventTimestamps[idx]
			timestampsInMillis[idx] = ts.AsTime().UnixNano() / int64(time.Millisecond)
			featureValues[idx] = newLog.FeatureValues[idx].String()
		}
		newParquetLog := ParquetLog{
			EntityName:      newLog.EntityName,
			EntityValue:     newLog.EntityValue.String(),
			FeatureNames:    newLog.FeatureNames,
			FeatureValues:   featureValues,
			FeatureStatuses: statuses,
			EventTimestamps: timestampsInMillis,
		}
		if err = pw.Write(newParquetLog); err != nil {
			return err
		}
	}
	if err = pw.WriteStop(); err != nil {
		return err
	}
	log.Println("Flushed Log to Parquet File Storage")
	w.Close()
	return nil
}
