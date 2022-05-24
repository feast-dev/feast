package logging

import (
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"

	"github.com/apache/arrow/go/v8/arrow"
	"github.com/apache/arrow/go/v8/arrow/array"
	"github.com/apache/arrow/go/v8/parquet"
	"github.com/apache/arrow/go/v8/parquet/pqarrow"
	"github.com/google/uuid"
)

type OfflineStoreWriteCallback func(featureServiceName, datasetDir string) string

type OfflineStoreSink struct {
	datasetDir    string
	writeCallback OfflineStoreWriteCallback
}

func NewOfflineStoreSink(writeCallback OfflineStoreWriteCallback) (*OfflineStoreSink, error) {
	return &OfflineStoreSink{
		datasetDir:    "",
		writeCallback: writeCallback,
	}, nil
}

func (s *OfflineStoreSink) getOrCreateDatasetDir() (string, error) {
	if s.datasetDir != "" {
		return s.datasetDir, nil
	}
	dir, err := ioutil.TempDir("", "*")
	if err != nil {
		return "", err
	}
	s.datasetDir = dir
	return s.datasetDir, nil
}

func (s *OfflineStoreSink) Write(records []arrow.Record) error {
	fileName, _ := uuid.NewUUID()
	datasetDir, err := s.getOrCreateDatasetDir()
	if err != nil {
		return err
	}

	var writer io.Writer
	writer, err = os.Create(filepath.Join(datasetDir, fmt.Sprintf("%s.parquet", fileName.String())))
	if err != nil {
		return err
	}
	table := array.NewTableFromRecords(records[0].Schema(), records)

	props := parquet.NewWriterProperties(parquet.WithDictionaryDefault(false))
	arrProps := pqarrow.DefaultWriterProps()
	return pqarrow.WriteTable(table, writer, 1000, props, arrProps)
}

func (s *OfflineStoreSink) Flush(featureServiceName string) error {
	if s.datasetDir == "" {
		return nil
	}

	datasetDir := s.datasetDir
	s.datasetDir = ""

	go func() {
		errMsg := s.writeCallback(featureServiceName, datasetDir)
		if errMsg != "" {
			log.Println(errMsg)
		}
		os.RemoveAll(datasetDir)
	}()

	return nil
}
