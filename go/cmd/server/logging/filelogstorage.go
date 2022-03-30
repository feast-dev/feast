package logging

import (
	"time"

	"github.com/feast-dev/feast/go/protos/feast/serving"
)

type FileLogStorage struct {
	// Feast project name
	project string
	path    string
}

type ParquetLog struct {
	EntityName      string   `parquet:"name=entityname, type=BYTE_ARRAY"`
	FeatureNames    []string `parquet:"name=featurenames, type=MAP, convertedtype=LIST, valuetype=BYTE_ARRAY, valueconvertedtype=UTF8"`
	FeatureStatuses []bool   `parquet:"name=featurestatuses, type=BOOLEAN, repetitiontype=REPEATED"`
	EventTimestamps []int64  `parquet:"name=eventtimestamps, type=INT64, repetitiontype=REPEATED, convertedtype=TIMESTAMP_MILLIS"`
}

func NewFileOfflineStore(project string, offlineStoreConfig map[string]interface{}) (*FileLogStorage, error) {
	store := FileLogStorage{project: project}
	return &store, nil
}

func (f *FileLogStorage) FlushToStorage(m *MemoryBuffer) error {
	if len(m.logs) == 0 {
		return nil
	}
	for _, log := range m.logs {
		numValues := len(log.FeatureValues)
		statuses := make([]bool, numValues)
		timestampsInMillis := make([]int64, numValues)
		for idx := 0; idx < numValues; idx++ {
			if log.FeatureStatuses[idx] == serving.FieldStatus_PRESENT {
				statuses[idx] = true
			} else {
				statuses[idx] = false
			}
			ts := log.EventTimestamps[idx]
			timestampsInMillis[idx] = ts.AsTime().UnixNano() / int64(time.Millisecond)
		}
		newParquetLog := ParquetLog{
			EntityName:      log.EntityName,
			FeatureNames:    log.FeatureNames,
			FeatureStatuses: statuses,
			EventTimestamps: timestampsInMillis,
		}
	}
	return nil
}
