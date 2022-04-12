package logging

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/apache/arrow/go/v8/arrow/array"
	"github.com/apache/arrow/go/v8/parquet"
	"github.com/apache/arrow/go/v8/parquet/pqarrow"
	"github.com/feast-dev/feast/go/internal/feast/registry"
)

type FileLogStorage struct {
	// Feast project name
	project string
	path    string
}

func GetFileConfig(config *registry.RepoConfig) (*OfflineLogStoreConfig, error) {
	fileConfig := OfflineLogStoreConfig{
		storeType: "file",
	}
	if onlineStorePath, ok := config.OfflineStore["path"]; ok {
		path, success := onlineStorePath.(string)
		if !success {
			return &fileConfig, fmt.Errorf("path, %s, cannot be converted to string", path)
		}
		fileConfig.path = path
	} else {
		return nil, errors.New("need path for file log storage")
	}
	return &fileConfig, nil
}

// This offline store is currently only used for testing. It will be instantiated during go unit tests to log to file
// and the parquet files will be cleaned up after the test is run.
func NewFileOfflineStore(project string, offlineStoreConfig *OfflineLogStoreConfig) (*FileLogStorage, error) {
	store := FileLogStorage{project: project}
	var absPath string
	var err error
	// TODO(kevjumba) remove this default catch.
	if offlineStoreConfig.path != "" {
		absPath, err = filepath.Abs(offlineStoreConfig.path)
	} else {
		return nil, errors.New("need path for file log storage")
	}
	if err != nil {
		return nil, err
	}
	store.path = absPath
	return &store, nil
}

func OpenLogFile(absPath string) (*os.File, error) {
	var _, err = os.Stat(absPath)

	// create file if not exists
	if os.IsNotExist(err) {
		var file, err = os.Create(absPath)
		if err != nil {
			return nil, err
		}
		return file, nil
	} else {
		return nil, fmt.Errorf("path %s already exists", absPath)
	}
}

func (f *FileLogStorage) FlushToStorage(tbl array.Table) error {
	w, err := OpenLogFile(f.path)
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
