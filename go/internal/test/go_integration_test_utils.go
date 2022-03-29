package test

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"time"

	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/reader"
	"github.com/xitongsys/parquet-go/source"
)

var jsonSchema string = `
{
	"Tag": "name=Schema, repetitiontype=REQUIRED",
	"Fields": [
	  {
		"Tag": "name=Event_timestamp, type=INT64, convertedtype=TIMESTAMP_MICROS, repetitiontype=OPTIONAL"
	  },
	  {
		"Tag": "name=Driver_id, type=INT64, repetitiontype=OPTIONAL"
	  },
	  {
		"Tag": "name=Conv_rate, type=FLOAT, repetitiontype=OPTIONAL"
	  },
	  {
		"Tag": "name=Acc_rate, type=FLOAT, repetitiontype=OPTIONAL"
	  },
	  {
		"Tag": "name=Avg_daily_trips, type=INT32, repetitiontype=OPTIONAL"
	  },
	  {
		"Tag": "name=Created, type=INT64, convertedtype=TIMESTAMP_MICROS, repetitiontype=OPTIONAL"
	  }
	]
}
`

type Row struct {
	Event_timestamp int64   `json:"Event_timestamp"`
	Driver_id       int64   `json:"Driver_id"`
	Conv_rate       float32 `json:"Conv_rate"`
	Acc_rate        float32 `json:"Acc_rate"`
	Avg_daily_trips int32   `json:"Avg_daily_trips"`
	Created         int64   `json:"Created"`
}

func ReadParquet(filePath string) ([]*Row, error) {
	var fr source.ParquetFile

	fr, err := local.NewLocalFileReader(filePath)
	if err != nil {
		return nil, err
	}
	pr, err := reader.NewParquetReader(fr, nil, 1)
	if err != nil {
		log.Printf("Can't create parquet reader")
		return nil, err
	}
	if err = pr.SetSchemaHandlerFromJSON(jsonSchema); err != nil {
		log.Println("Can't set schema from json", err)
		return nil, err
	}

	num := int(pr.GetNumRows())
	res, err := pr.ReadByNumber(num)
	if err != nil {
		return nil, err
	}
	jsonBs, err := json.Marshal(res)
	if err != nil {
		return nil, err
	}

	Rows := []*Row{}
	err = json.Unmarshal(jsonBs, &Rows)
	if err != nil {
		return nil, err
	}
	return Rows, nil
}

func GetLatestFeatures(Rows []*Row, entities map[int64]bool) map[int64]*Row {
	correctFeatureRows := make(map[int64]*Row)
	for _, Row := range Rows {
		if _, ok := entities[Row.Driver_id]; ok {
			if _, ok := correctFeatureRows[Row.Driver_id]; ok {
				if Row.Event_timestamp > correctFeatureRows[Row.Driver_id].Event_timestamp {
					correctFeatureRows[Row.Driver_id] = Row
				}
			} else {
				correctFeatureRows[Row.Driver_id] = Row
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
		return err
	}
	return nil
}
