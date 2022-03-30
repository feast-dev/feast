package test

import (
	"context"
	"fmt"
	"github.com/apache/arrow/go/v8/arrow/array"
	"github.com/apache/arrow/go/v8/arrow/memory"

	"github.com/apache/arrow/go/v8/arrow"
	"github.com/apache/arrow/go/v8/parquet/file"
	"github.com/apache/arrow/go/v8/parquet/pqarrow"

	"os"
	"os/exec"
	"path/filepath"
	"time"
)

type Row struct {
	EventTimestamp int64
	DriverId       int64
	ConvRate       float32
	AccRate        float32
	AvgDailyTrips  int32
	Created        int64
}

func ReadParquet(filePath string) ([]*Row, error) {
	allocator := memory.NewGoAllocator()
	pqfile, err := file.OpenParquetFile(filePath, false)
	if err != nil {
		return nil, err
	}
	reader, err := pqarrow.NewFileReader(pqfile, pqarrow.ArrowReadProperties{}, allocator)
	if err != nil {
		return nil, err
	}
	fmt.Println(reader)
	table, err := reader.ReadTable(context.Background())
	if err != nil {
		return nil, err
	}

	columns := make(map[string]arrow.Array)
	fields := table.Schema().Fields()
	for idx, field := range fields {
		columns[field.Name] = table.Column(idx).Data().Chunk(0)
	}

	rows := make([]*Row, 0)
	for rowIdx := 0; rowIdx < int(table.NumRows()); rowIdx++ {
		rows = append(rows, &Row{
			EventTimestamp: columns["event_timestamp"].(*array.Timestamp).Value(rowIdx).ToTime(arrow.Second).Unix(),
			DriverId:       columns["driver_id"].(*array.Int64).Value(rowIdx),
			ConvRate:       columns["conv_rate"].(*array.Float32).Value(rowIdx),
			AccRate:        columns["acc_rate"].(*array.Float32).Value(rowIdx),
			AvgDailyTrips:  columns["avg_daily_trips"].(*array.Int32).Value(rowIdx),
			Created:        columns["created"].(*array.Timestamp).Value(rowIdx).ToTime(arrow.Second).Unix(),
		})
	}

	return rows, nil
}

func GetLatestFeatures(Rows []*Row, entities map[int64]bool) map[int64]*Row {
	correctFeatureRows := make(map[int64]*Row)
	for _, Row := range Rows {
		if _, ok := entities[Row.DriverId]; ok {
			if _, ok := correctFeatureRows[Row.DriverId]; ok {
				if Row.EventTimestamp > correctFeatureRows[Row.DriverId].EventTimestamp {
					correctFeatureRows[Row.DriverId] = Row
				}
			} else {
				correctFeatureRows[Row.DriverId] = Row
			}
		}
	}
	return correctFeatureRows
}

func SetupFeatureRepo(basePath string) error {
	cmd := exec.Command("feast", "init", "feature_repo")
	path, err := filepath.Abs(basePath)
	cmd.Env = os.Environ()

	if err != nil {
		return err
	}
	cmd.Dir = path
	err = cmd.Run()
	if err != nil {
		return err
	}
	applyCommand := exec.Command("feast", "apply")
	applyCommand.Env = os.Environ()
	feature_repo_path, err := filepath.Abs(filepath.Join(path, "feature_repo"))
	if err != nil {
		return err
	}
	applyCommand.Dir = feature_repo_path
	err = applyCommand.Run()
	if err != nil {
		return err
	}
	t := time.Now()

	formattedTime := fmt.Sprintf("%d-%02d-%02dT%02d:%02d:%02d",
		t.Year(), t.Month(), t.Day(),
		t.Hour(), t.Minute(), t.Second())
	materializeCommand := exec.Command("feast", "materialize-incremental", formattedTime)
	materializeCommand.Env = os.Environ()
	materializeCommand.Dir = feature_repo_path
	err = materializeCommand.Run()
	if err != nil {
		return err
	}
	return nil
}

func CleanUpRepo(basePath string) error {
	feature_repo_path, err := filepath.Abs(filepath.Join(basePath, "feature_repo"))
	if err != nil {
		return err
	}
	err = os.RemoveAll(feature_repo_path)
	if err != nil {
		log.Fatalf("Couldn't remove feature repo path. %s", err)
		return err
	}
	return nil
}
