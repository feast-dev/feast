package test

import (
	"context"
	"fmt"
	"log"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/apache/arrow/go/v17/parquet/file"
	"github.com/apache/arrow/go/v17/parquet/pqarrow"

	"os"
	"os/exec"
	"path/filepath"
	"time"

	"github.com/apache/arrow/go/v17/arrow/array"

	"github.com/feast-dev/feast/go/protos/feast/types"
	gotypes "github.com/feast-dev/feast/go/types"
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

func ReadParquetDynamically(filePath string) ([]map[string]interface{}, error) {
	allocator := memory.NewGoAllocator()
	pqfile, err := file.OpenParquetFile(filePath, false)
	if err != nil {
		return nil, err
	}
	defer pqfile.Close()

	reader, err := pqarrow.NewFileReader(pqfile, pqarrow.ArrowReadProperties{}, allocator)
	if err != nil {
		return nil, err
	}

	table, err := reader.ReadTable(context.Background())
	if err != nil {
		return nil, err
	}
	defer table.Release()

	// Create a map of column names to their data arrays
	columns := make(map[string]arrow.Array)
	fields := table.Schema().Fields()
	for idx, field := range fields {
		columns[field.Name] = table.Column(idx).Data().Chunk(0)
	}

	// Read rows dynamically
	rows := make([]map[string]interface{}, 0)
	for rowIdx := 0; rowIdx < int(table.NumRows()); rowIdx++ {
		row := make(map[string]interface{})
		for _, field := range fields {
			column := columns[field.Name]
			if column.IsNull(rowIdx) {
				row[field.Name] = nil // Set nil for null values
				continue
			}
			switch col := column.(type) {
			case *array.Int32:
				row[field.Name] = col.Value(rowIdx)
			case *array.Int64:
				row[field.Name] = col.Value(rowIdx)
			case *array.Float32:
				row[field.Name] = col.Value(rowIdx)
			case *array.Float64:
				row[field.Name] = col.Value(rowIdx)
			case *array.String:
				row[field.Name] = col.Value(rowIdx)
			case *array.Boolean:
				row[field.Name] = col.Value(rowIdx)
			case *array.Binary:
				row[field.Name] = col.Value(rowIdx)
			case *array.Timestamp:
				nanoseconds := int64(col.Value(rowIdx).ToTime(arrow.Second).Unix())
				t := time.Unix(0, nanoseconds)
				row[field.Name] = t.Unix()
			case *array.List:
				// Handle array (list) types
				listValues := []interface{}{}
				list := col.ListValues()
				for i := col.Offsets()[rowIdx]; i < col.Offsets()[rowIdx+1]; i++ {
					switch childCol := list.(type) {
					case *array.Int32:
						listValues = append(listValues, childCol.Value(int(i)))
					case *array.Int64:
						listValues = append(listValues, childCol.Value(int(i)))
					case *array.Float32:
						listValues = append(listValues, childCol.Value(int(i)))
					case *array.Float64:
						listValues = append(listValues, childCol.Value(int(i)))
					case *array.String:
						listValues = append(listValues, childCol.Value(int(i)))
					case *array.Boolean:
						listValues = append(listValues, childCol.Value(int(i)))
					case *array.Binary:
						listValues = append(listValues, childCol.Value(int(i)))
					case *array.Timestamp:
						nanoseconds := int64(childCol.Value(int(i)).ToTime(arrow.Second).Unix())
						t := time.Unix(0, nanoseconds)
						listValues = append(listValues, t.Unix())
					default:
						listValues = append(listValues, nil) // Handle unsupported types
					}
				}
				row[field.Name] = listValues
			default:
				fmt.Println("Unsupported type:", field.Name, field.Type, col)
				row[field.Name] = nil // Handle unsupported types
			}
		}
		rows = append(rows, row)
	}

	return rows, nil
}

func FilterRowsByColumn(rows []map[string]interface{}, columnName string, value interface{}) []map[string]interface{} {
	filteredRows := []map[string]interface{}{}
	for _, row := range rows {
		if row[columnName] == value {
			filteredRows = append(filteredRows, row)
		}
	}
	return filteredRows
}

func FilterRowsByMultiColumns(rows []map[string]interface{}, filters map[string]interface{}) []map[string]interface{} {
	filteredRows := []map[string]interface{}{}
	for _, row := range rows {
		matches := true
		for columnName, value := range filters {
			if row[columnName] != value {
				matches = false
				break
			}
		}
		if matches {
			filteredRows = append(filteredRows, row)
		}
	}
	return filteredRows
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

func SetupCleanFeatureRepo(basePath string) error {
	cmd := exec.Command("feast", "init", "my_project")
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
	featureRepoPath, err := filepath.Abs(filepath.Join(path, "my_project", "feature_repo"))
	if err != nil {
		return err
	}
	applyCommand.Dir = featureRepoPath
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
	materializeCommand.Dir = featureRepoPath
	err = materializeCommand.Run()
	if err != nil {
		return err
	}
	return nil
}

func SetupInitializedRepo(basePath string) error {
	path, err := filepath.Abs(basePath)
	if err != nil {
		return err
	}
	applyCommand := exec.Command("feast", "apply")
	applyCommand.Env = os.Environ()
	featureRepoPath, err := filepath.Abs(filepath.Join(path, "feature_repo"))
	if err != nil {
		return err
	}
	// var stderr bytes.Buffer
	// var stdout bytes.Buffer
	applyCommand.Dir = featureRepoPath
	out, err := applyCommand.CombinedOutput()
	if err != nil {
		log.Println(string(out))
		return err
	}
	t := time.Now()

	formattedTime := fmt.Sprintf("%d-%02d-%02dT%02d:%02d:%02d",
		t.Year(), t.Month(), t.Day(),
		t.Hour(), t.Minute(), t.Second())

	materializeCommand := exec.Command("feast", "materialize-incremental", formattedTime)
	materializeCommand.Env = os.Environ()
	materializeCommand.Dir = featureRepoPath
	out, err = materializeCommand.CombinedOutput()
	if err != nil {
		log.Println(string(out))
		return err
	}
	return nil
}

func CleanUpInitializedRepo(basePath string) {
	featureRepoPath, err := filepath.Abs(filepath.Join(basePath, "feature_repo"))
	if err != nil {
		log.Fatal(err)
	}

	if _, err := os.Stat(filepath.Join(featureRepoPath, "data", "registry.db")); err == nil {
		err = os.Remove(filepath.Join(featureRepoPath, "data", "registry.db"))
		if err != nil {
			log.Fatal(err)
		}
	}

	if _, err := os.Stat(filepath.Join(featureRepoPath, "data", "online_store.db")); err == nil {
		err = os.Remove(filepath.Join(featureRepoPath, "data", "online_store.db"))
		if err != nil {
			log.Fatal(err)
		}
	}
}

func GetProtoFromRecord(rec arrow.Record) (map[string]*types.RepeatedValue, error) {
	r := make(map[string]*types.RepeatedValue)
	schema := rec.Schema()
	for idx, column := range rec.Columns() {
		field := schema.Field(idx)
		if field.Type.ID() == arrow.FixedWidthTypes.Timestamp_ms.ID() || field.Type.ID() == arrow.FixedWidthTypes.Date32.ID() {
			continue
		}
		values, err := gotypes.ArrowValuesToProtoValues(column)
		if err != nil {
			return nil, err
		}
		r[field.Name] = &types.RepeatedValue{Val: values}
	}
	return r, nil
}
