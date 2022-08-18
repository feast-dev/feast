package test

import (
	"context"
	"fmt"
	"log"

	"github.com/apache/arrow/go/v8/arrow/memory"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/apache/arrow/go/v8/arrow"
	"github.com/apache/arrow/go/v8/parquet/file"
	"github.com/apache/arrow/go/v8/parquet/pqarrow"

	"os"
	"os/exec"
	"path/filepath"
	"time"

	"github.com/apache/arrow/go/v8/arrow/array"

	"github.com/feast-dev/feast/go/internal/feast/model"
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
	applyCommand.Run()
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

	err = os.Remove(filepath.Join(featureRepoPath, "data", "registry.db"))
	if err != nil {
		log.Fatal(err)
	}
	err = os.Remove(filepath.Join(featureRepoPath, "data", "online_store.db"))
	if err != nil {
		log.Fatal(err)
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

func CreateBaseFeatureView(name string, features []*model.Field, projection *model.FeatureViewProjection) *model.BaseFeatureView {
	return &model.BaseFeatureView{
		Name:       name,
		Features:   features,
		Projection: projection,
	}
}

func CreateNewEntity(name string, joinKey string) *model.Entity {
	return &model.Entity{
		Name:    name,
		JoinKey: joinKey,
	}
}

func CreateNewField(name string, dtype types.ValueType_Enum) *model.Field {
	return &model.Field{Name: name,
		Dtype: dtype,
	}
}

func CreateNewFeatureService(name string, project string, createdTimestamp *timestamppb.Timestamp, lastUpdatedTimestamp *timestamppb.Timestamp, projections []*model.FeatureViewProjection) *model.FeatureService {
	return &model.FeatureService{
		Name:                 name,
		Project:              project,
		CreatedTimestamp:     createdTimestamp,
		LastUpdatedTimestamp: lastUpdatedTimestamp,
		Projections:          projections,
	}
}

func CreateNewFeatureViewProjection(name string, nameAlias string, features []*model.Field, joinKeyMap map[string]string) *model.FeatureViewProjection {
	return &model.FeatureViewProjection{Name: name,
		NameAlias:  nameAlias,
		Features:   features,
		JoinKeyMap: joinKeyMap,
	}
}

func CreateFeatureView(base *model.BaseFeatureView, ttl *durationpb.Duration, entities []string, entityColumns []*model.Field) *model.FeatureView {
	return &model.FeatureView{
		Base:          base,
		Ttl:           ttl,
		EntityNames:   entities,
		EntityColumns: entityColumns,
	}
}
