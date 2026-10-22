package onlinestore

import (
	"crypto/sha1"
	"database/sql"
	"encoding/hex"
	"errors"
	"strings"
	"sync"
	"time"

	"github.com/feast-dev/feast/go/internal/feast/registry"

	"context"
	"fmt"

	_ "github.com/mattn/go-sqlite3"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/feast-dev/feast/go/protos/feast/serving"
	"github.com/feast-dev/feast/go/protos/feast/types"
)

type SqliteOnlineStore struct {
	// Feast project name
	project    string
	path       string
	db         *sql.DB
	db_mu      sync.Mutex
	repoConfig *registry.RepoConfig
}

// Creates a new sqlite online store object. onlineStoreConfig should have relative path of database file with respect to repoConfig.repoPath.
func NewSqliteOnlineStore(project string, repoConfig *registry.RepoConfig, onlineStoreConfig map[string]interface{}) (*SqliteOnlineStore, error) {
	store := SqliteOnlineStore{project: project, repoConfig: repoConfig}
	if db_path, ok := onlineStoreConfig["path"]; !ok {
		return nil, fmt.Errorf("cannot find sqlite path %s", db_path)
	} else {
		if dbPathStr, ok := db_path.(string); !ok {
			return nil, fmt.Errorf("cannot find convert sqlite path to string %s", db_path)
		} else {
			store.path = fmt.Sprintf("%s/%s", repoConfig.RepoPath, dbPathStr)
			db, err := initializeConnection(store.path)
			if err != nil {
				return nil, err
			}
			store.db = db
		}
	}

	return &store, nil
}

func (s *SqliteOnlineStore) Destruct() {
	s.db.Close()
}

// Returns FeatureData 2D array. Each row corresponds to one entity Value and each column corresponds to a single feature where the number of columns should be
// same length as the length of featureNames. Reads from every table in featureViewNames with the entity keys described.
func (s *SqliteOnlineStore) OnlineRead(ctx context.Context, entityKeys []*types.EntityKey, featureViewNames []string, featureNames []string) ([][]FeatureData, error) {
	featureCount := len(featureNames)
	_, err := s.getConnection()
	if err != nil {
		return nil, err
	}
	project := s.project
	results := make([][]FeatureData, len(entityKeys))
	entityNameToEntityIndex := make(map[string]int)
	in_query := make([]string, len(entityKeys))
	serialized_entities := make([]interface{}, len(entityKeys))
	for i := 0; i < len(entityKeys); i++ {
		serKey, err := serializeEntityKey(entityKeys[i], s.repoConfig.EntityKeySerializationVersion)
		if err != nil {
			return nil, err
		}
		// TODO: fix this, string conversion is not safe
		entityNameToEntityIndex[hashSerializedEntityKey(serKey)] = i
		// for IN clause in read query
		in_query[i] = "?"
		serialized_entities[i] = *serKey
	}
	featureNamesToIdx := make(map[string]int)
	for idx, name := range featureNames {
		featureNamesToIdx[name] = idx
	}

	for _, featureViewName := range featureViewNames {
		query_string := fmt.Sprintf(`SELECT entity_key, feature_name, Value, event_ts
									FROM %s
									WHERE entity_key IN (%s)
									ORDER BY entity_key`, tableId(project, featureViewName), strings.Join(in_query, ","))
		rows, err := s.db.Query(query_string, serialized_entities...)
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		for rows.Next() {
			var entity_key []byte
			var feature_name string
			var valueString []byte
			var event_ts time.Time
			var value types.Value
			err = rows.Scan(&entity_key, &feature_name, &valueString, &event_ts)
			if err != nil {
				return nil, errors.New("error could not resolve row in query (entity key, feature name, value, event ts)")
			}
			if err := proto.Unmarshal(valueString, &value); err != nil {
				return nil, errors.New("error converting parsed value to types.Value")
			}
			rowIdx := entityNameToEntityIndex[hashSerializedEntityKey(&entity_key)]
			if results[rowIdx] == nil {
				results[rowIdx] = make([]FeatureData, featureCount)
			}
			results[rowIdx][featureNamesToIdx[feature_name]] = FeatureData{Reference: serving.FeatureReferenceV2{FeatureViewName: featureViewName, FeatureName: feature_name},
				Timestamp: *timestamppb.New(event_ts),
				Value:     types.Value{Val: value.Val},
			}
		}
	}
	return results, nil
}

// Gets a sqlite connection and sets it to the online store and also returns a pointer to the connection.
func (s *SqliteOnlineStore) getConnection() (*sql.DB, error) {
	s.db_mu.Lock()
	defer s.db_mu.Unlock()
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

// Constructs the table id from the project and table(featureViewName) string.
func tableId(project string, featureViewName string) string {
	return fmt.Sprintf("%s_%s", project, featureViewName)
}

// Creates a connection to the sqlite database and returns the connection.
func initializeConnection(db_path string) (*sql.DB, error) {
	db, err := sql.Open("sqlite3", db_path)
	if err != nil {
		return nil, err
	}
	return db, nil
}

func hashSerializedEntityKey(serializedEntityKey *[]byte) string {
	if serializedEntityKey == nil {
		return ""
	}
	h := sha1.New()
	h.Write(*serializedEntityKey)
	sha1_hash := hex.EncodeToString(h.Sum(nil))
	return sha1_hash
}
