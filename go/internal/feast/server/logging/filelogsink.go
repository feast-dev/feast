package logging

import (
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/pkg/errors"

	"github.com/apache/arrow/go/v8/arrow"
	"github.com/google/uuid"

	"github.com/apache/arrow/go/v8/arrow/array"
	"github.com/apache/arrow/go/v8/parquet"
	"github.com/apache/arrow/go/v8/parquet/pqarrow"
)

type FileLogSink struct {
	path string
}

// FileLogSink is currently only used for testing. It will be instantiated during go unit tests to log to file
// and the parquet files will be cleaned up after the test is run.
func NewFileLogSink(path string) (*FileLogSink, error) {
	if path == "" {
		return nil, errors.New("need path for file log sink")
	}

	absPath, err := filepath.Abs(path)
	if err != nil {
		return nil, err
	}
	return &FileLogSink{path: absPath}, nil
}

func (s *FileLogSink) Write(records []arrow.Record) error {
	fileName, _ := uuid.NewUUID()

	var writer io.Writer
	writer, err := os.Create(filepath.Join(s.path, fmt.Sprintf("%s.parquet", fileName.String())))
	if err != nil {
		return err
	}
	table := array.NewTableFromRecords(records[0].Schema(), records)

	props := parquet.NewWriterProperties(parquet.WithDictionaryDefault(false))
	arrProps := pqarrow.DefaultWriterProps()
	return pqarrow.WriteTable(table, writer, 100, props, arrProps)
}

func (s *FileLogSink) Flush(featureServiceName string) error {
	// files are already flushed during Write
	return nil
}
