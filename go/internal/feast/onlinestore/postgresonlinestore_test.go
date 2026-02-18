package onlinestore

import (
	"testing"

	"github.com/feast-dev/feast/go/internal/feast/registry"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
)

func TestBuildPostgresConnString(t *testing.T) {
	config := map[string]interface{}{
		"type":     "postgres",
		"host":     "db.example.com",
		"port":     float64(5432),
		"database": "feast",
		"user":     "feast_user",
		"password": "feast_pass",
		"sslmode":  "require",
	}
	connStr := buildPostgresConnString(config)
	assert.Contains(t, connStr, "db.example.com:5432")
	assert.Contains(t, connStr, "feast_user:")
	assert.Contains(t, connStr, "feast_pass@")
	assert.Contains(t, connStr, "/feast")
	assert.Contains(t, connStr, "sslmode=require")
}

func TestBuildPostgresConnStringDefaults(t *testing.T) {
	config := map[string]interface{}{
		"database": "feast",
		"user":     "feast_user",
		"password": "feast_pass",
	}
	connStr := buildPostgresConnString(config)
	assert.Contains(t, connStr, "localhost:5432")
	assert.Contains(t, connStr, "sslmode=disable")
}

func TestBuildPostgresConnStringWithSSL(t *testing.T) {
	config := map[string]interface{}{
		"host":             "db.example.com",
		"port":             float64(5433),
		"database":         "feast",
		"user":             "feast_user",
		"password":         "feast_pass",
		"sslmode":          "verify-full",
		"sslcert_path":     "/path/to/cert",
		"sslkey_path":      "/path/to/key",
		"sslrootcert_path": "/path/to/rootcert",
	}
	connStr := buildPostgresConnString(config)
	assert.Contains(t, connStr, "db.example.com:5433")
	assert.Contains(t, connStr, "sslmode=verify-full")
	assert.Contains(t, connStr, "sslcert=%2Fpath%2Fto%2Fcert")
	assert.Contains(t, connStr, "sslkey=%2Fpath%2Fto%2Fkey")
	assert.Contains(t, connStr, "sslrootcert=%2Fpath%2Fto%2Frootcert")
}

func TestBuildPostgresConnStringWithPooling(t *testing.T) {
	config := map[string]interface{}{
		"host":     "localhost",
		"database": "feast",
		"user":     "feast_user",
		"password": "feast_pass",
		"min_conn": float64(2),
		"max_conn": float64(10),
	}
	connStr := buildPostgresConnString(config)
	assert.Contains(t, connStr, "pool_min_conns=2")
	assert.Contains(t, connStr, "pool_max_conns=10")
}

func TestBuildPostgresConnStringSpecialChars(t *testing.T) {
	config := map[string]interface{}{
		"host":     "localhost",
		"database": "feast",
		"user":     "user@domain",
		"password": "p@ss=word&special",
	}
	connStr := buildPostgresConnString(config)
	poolConfig, err := pgxpool.ParseConfig(connStr)
	assert.Nil(t, err)
	assert.Equal(t, "user@domain", poolConfig.ConnConfig.User)
	assert.Equal(t, "p@ss=word&special", poolConfig.ConnConfig.Password)
}

func TestBuildPostgresConnStringParseable(t *testing.T) {
	config := map[string]interface{}{
		"host":     "localhost",
		"port":     float64(5432),
		"database": "feast",
		"user":     "feast_user",
		"password": "feast_pass",
	}
	connStr := buildPostgresConnString(config)
	poolConfig, err := pgxpool.ParseConfig(connStr)
	assert.Nil(t, err)
	assert.Equal(t, "localhost", poolConfig.ConnConfig.Host)
	assert.Equal(t, uint16(5432), poolConfig.ConnConfig.Port)
	assert.Equal(t, "feast", poolConfig.ConnConfig.Database)
	assert.Equal(t, "feast_user", poolConfig.ConnConfig.User)
	assert.Equal(t, "feast_pass", poolConfig.ConnConfig.Password)
}

func TestNewPostgresOnlineStore(t *testing.T) {
	config := map[string]interface{}{
		"type":     "postgres",
		"host":     "localhost",
		"port":     float64(5432),
		"database": "feast",
		"user":     "feast_user",
		"password": "feast_pass",
	}
	rc := &registry.RepoConfig{
		OnlineStore:                   config,
		EntityKeySerializationVersion: 2,
	}
	store, err := NewPostgresOnlineStore("test", rc, config)
	assert.Nil(t, err)
	assert.NotNil(t, store)
	assert.Equal(t, "feast", store.pool.Config().ConnConfig.Database)
	store.Destruct()
}

func TestNewPostgresOnlineStoreWithSchema(t *testing.T) {
	config := map[string]interface{}{
		"type":      "postgres",
		"host":      "localhost",
		"port":      float64(5432),
		"database":  "feast",
		"user":      "feast_user",
		"password":  "feast_pass",
		"db_schema": "custom_schema",
	}
	rc := &registry.RepoConfig{
		OnlineStore:                   config,
		EntityKeySerializationVersion: 2,
	}
	store, err := NewPostgresOnlineStore("test", rc, config)
	assert.Nil(t, err)
	assert.NotNil(t, store)
	assert.Equal(t, "custom_schema", store.pool.Config().ConnConfig.RuntimeParams["search_path"])
	store.Destruct()
}
