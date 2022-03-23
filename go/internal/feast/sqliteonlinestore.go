package feast

import (
	"context"
	"fmt"

	"github.com/feast-dev/feast/go/protos/feast/types"
)

type SqliteOnlineStore struct {
	// Feast project name
	// TODO (woop): Should we remove project as state that is tracked at the store level?
	project string

	path string
}

func NewSqliteOnlineStore(project string, onlineStoreConfig map[string]interface{}) (*SqliteOnlineStore, error) {
	store := SqliteOnlineStore{project: project}
	if db_path, ok := onlineStoreConfig["path"]; !ok {
		return nil, fmt.Errorf("cannot find sqlite path %s", db_path)
	} else if dbPathStr, ok := db_path.(string); !ok {
		return nil, fmt.Errorf("cannot find convert sqlite path to string %s", db_path)
	} else {
		store.path = dbPathStr
	}
	return &SqliteOnlineStore{
		project: project,
	}, nil
}

func (*SqliteOnlineStore) Destruct() {

}

func (*SqliteOnlineStore) OnlineRead(ctx context.Context, entityKeys []types.EntityKey, featureViewNames []string, featureNames []string) ([][]FeatureData, error) {
	return nil, nil
}

// feature views, entities, dataframe?
func (*SqliteOnlineStore) WriteToOnlineStore(ctx context.Context, featureViewName string, data [][]FeatureData) error {
	return nil
}

func (*SqliteOnlineStore) initializeConnection(db_path string) {

}
