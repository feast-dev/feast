package feast

import (
	"database/sql"
	"errors"

	"context"
	"fmt"

	"github.com/feast-dev/feast/go/protos/feast/types"
	_ "github.com/mattn/go-sqlite3"
)

type SqliteOnlineStore struct {
	// Feast project name
	// TODO (woop): Should we remove project as state that is tracked at the store level?
	project string

	path string
	db   *sql.DB
}

func NewSqliteOnlineStore(project string, onlineStoreConfig map[string]interface{}) (*SqliteOnlineStore, error) {
	store := SqliteOnlineStore{project: project}
	if db_path, ok := onlineStoreConfig["path"]; !ok {
		return nil, fmt.Errorf("cannot find sqlite path %s", db_path)
	} else if dbPathStr, ok := db_path.(string); !ok {
		return nil, fmt.Errorf("cannot find convert sqlite path to string %s", db_path)
	} else {
		store.path = dbPathStr
		db, err := initializeConnection(dbPathStr)
		if err != nil {
			return nil, err
		}
		store.db = db
	}
	return &store, nil
}

func (s *SqliteOnlineStore) Destruct() {
	s.db.Close()
}

func (s *SqliteOnlineStore) OnlineRead(ctx context.Context, entityKeys []types.EntityKey, featureViewNames []string, featureNames []string) ([][]FeatureData, error) {
	_, err := s.getConnection()
	if err != nil {
		return nil, err
	}
	return nil, nil
}

// feature views, entities, dataframe?
func (s *SqliteOnlineStore) WriteToOnlineStore(ctx context.Context, featureViewName string, data [][]FeatureData) error {
	return nil
}

func (s *SqliteOnlineStore) Update(ctx context.Context, config *RepoConfig, tables_to_delete []*FeatureView, tables_to_keep []*FeatureView) error {
	_, err := s.getConnection()
	if err != nil {
		return err
	}
	project := config.Project
	for _, table := range tables_to_keep {
		s.db.Exec(
			fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (entity_key BLOB, feature_name TEXT, value BLOB, event_ts timestamp, created_ts timestamp,  PRIMARY KEY(entity_key, feature_name))", tableId(project, table)))
		s.db.Exec(
			fmt.Sprintf("CREATE INDEX IF NOT EXISTS %s_ek ON %s (entity_key);", tableId(project, table), tableId(project, table)))
	}
	for _, table := range tables_to_delete {
		s.db.Exec("DROP TABLE IF EXISTS %s", tableId(project, table))
	}
	return nil
}

func (s *SqliteOnlineStore) getConnection() (*sql.DB, error) {
	if s.db == nil {
		if s.path == "" {
			return nil, errors.New("no database path available")
		}
		db, err := initializeConnection(s.path)
		s.db = db
		if err != nil {
			return nil, err
		}
	}
	return s.db, nil
}

func tableId(project string, table *FeatureView) string {
	return fmt.Sprintf("%s_%s", project, table.base.name)
}

func initializeConnection(db_path string) (*sql.DB, error) {
	db, err := sql.Open("sqlite3", "./foo.db")
	if err != nil {
		return nil, err
	}
	return db, nil
}
