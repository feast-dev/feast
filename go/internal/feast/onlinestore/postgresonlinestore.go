package onlinestore

import (
	"context"
	"fmt"
	"net/url"
	"time"

	"github.com/feast-dev/feast/go/internal/feast/registry"
	"github.com/feast-dev/feast/go/protos/feast/serving"
	"github.com/feast-dev/feast/go/protos/feast/types"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type PostgresOnlineStore struct {
	project string
	pool    *pgxpool.Pool
	config  *registry.RepoConfig
}

func NewPostgresOnlineStore(project string, config *registry.RepoConfig, onlineStoreConfig map[string]interface{}) (*PostgresOnlineStore, error) {
	connStr := buildPostgresConnString(onlineStoreConfig)
	poolConfig, err := pgxpool.ParseConfig(connStr)
	if err != nil {
		return nil, fmt.Errorf("failed to parse postgres config: %w", err)
	}

	if schema, ok := onlineStoreConfig["db_schema"].(string); ok && schema != "" {
		poolConfig.ConnConfig.RuntimeParams["search_path"] = schema
	}

	pool, err := pgxpool.NewWithConfig(context.Background(), poolConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create postgres pool: %w", err)
	}

	return &PostgresOnlineStore{
		project: project,
		pool:    pool,
		config:  config,
	}, nil
}

func (p *PostgresOnlineStore) Destruct() {
	if p.pool != nil {
		p.pool.Close()
	}
}

func (p *PostgresOnlineStore) OnlineRead(ctx context.Context, entityKeys []*types.EntityKey, featureViewNames []string, featureNames []string) ([][]FeatureData, error) {
	featureCount := len(featureNames)
	results := make([][]FeatureData, len(entityKeys))

	serializedKeys := make([][]byte, len(entityKeys))
	entityKeyMap := make(map[string]int, len(entityKeys))

	for i, entityKey := range entityKeys {
		serKey, err := serializeEntityKey(entityKey, p.config.EntityKeySerializationVersion)
		if err != nil {
			return nil, err
		}
		serializedKeys[i] = *serKey
		entityKeyMap[string(*serKey)] = i
	}

	featureNamesToIdx := make(map[string]int, len(featureNames))
	for idx, name := range featureNames {
		featureNamesToIdx[name] = idx
	}

	for _, featureViewName := range featureViewNames {
		tableName := fmt.Sprintf(`"%s"`, tableId(p.project, featureViewName))
		query := fmt.Sprintf(
			`SELECT entity_key, feature_name, value, event_ts FROM %s WHERE entity_key = ANY($1)`,
			tableName,
		)

		rows, err := p.pool.Query(ctx, query, serializedKeys)
		if err != nil {
			return nil, fmt.Errorf("failed to query postgres: %w", err)
		}

		for rows.Next() {
			var entityKeyBytes []byte
			var featureName string
			var valueBytes []byte
			var eventTs time.Time

			if err := rows.Scan(&entityKeyBytes, &featureName, &valueBytes, &eventTs); err != nil {
				rows.Close()
				return nil, fmt.Errorf("failed to scan postgres row: %w", err)
			}

			rowIdx, ok := entityKeyMap[string(entityKeyBytes)]
			if !ok {
				continue
			}

			if results[rowIdx] == nil {
				results[rowIdx] = make([]FeatureData, featureCount)
			}

			if featureIdx, ok := featureNamesToIdx[featureName]; ok {
				var value types.Value
				if err := proto.Unmarshal(valueBytes, &value); err != nil {
					rows.Close()
					return nil, fmt.Errorf("failed to unmarshal feature value: %w", err)
				}

				results[rowIdx][featureIdx] = FeatureData{
					Reference: serving.FeatureReferenceV2{FeatureViewName: featureViewName, FeatureName: featureName},
					Timestamp: *timestamppb.New(eventTs),
					Value:     types.Value{Val: value.Val},
				}
			}
        }
        rows.Close()
        if err := rows.Err(); err != nil {
            return nil, fmt.Errorf("error iterating postgres rows: %w", err)
        }

	}

	return results, nil
}

func buildPostgresConnString(config map[string]interface{}) string {
	host, _ := config["host"].(string)
	if host == "" {
		host = "localhost"
	}

	port := 5432
	if p, ok := config["port"].(float64); ok {
		port = int(p)
	}

	database, _ := config["database"].(string)
	user, _ := config["user"].(string)
	password, _ := config["password"].(string)

	var userInfo *url.Userinfo
	if user != "" {
		if password != "" {
			userInfo = url.UserPassword(user, password)
		} else {
			userInfo = url.User(user)
		}
	}

	query := url.Values{}
	if sslMode, ok := config["sslmode"].(string); ok && sslMode != "" {
		query.Set("sslmode", sslMode)
	} else {
		query.Set("sslmode", "disable")
	}

	if v, ok := config["sslcert_path"].(string); ok && v != "" {
		query.Set("sslcert", v)
	}
	if v, ok := config["sslkey_path"].(string); ok && v != "" {
		query.Set("sslkey", v)
	}
	if v, ok := config["sslrootcert_path"].(string); ok && v != "" {
		query.Set("sslrootcert", v)
	}
	if v, ok := config["min_conn"].(float64); ok {
		query.Set("pool_min_conns", fmt.Sprintf("%d", int(v)))
	}
	if v, ok := config["max_conn"].(float64); ok {
		query.Set("pool_max_conns", fmt.Sprintf("%d", int(v)))
	}

	connURL := url.URL{
		Scheme:   "postgres",
		User:     userInfo,
		Host:     fmt.Sprintf("%s:%d", host, port),
		Path:     database,
		RawQuery: query.Encode(),
	}

	return connURL.String()
}