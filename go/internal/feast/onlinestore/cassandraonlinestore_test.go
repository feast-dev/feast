package onlinestore

import (
	"context"
	"github.com/gocql/gocql"
	"github.com/stretchr/testify/assert"
	"reflect"
	"testing"
)

func TestExtractCassandraConfig_CorrectDefaults(t *testing.T) {
	var config = map[string]interface{}{}
	cassandraConfig, _ := extractCassandraConfig(config)

	assert.Equal(t, []string{"127.0.0.1"}, cassandraConfig.hosts)
	assert.Equal(t, "", cassandraConfig.username)
	assert.Equal(t, "", cassandraConfig.password)
	assert.Equal(t, "feast_keyspace", cassandraConfig.keyspace)
	assert.Equal(t, 4, cassandraConfig.protocolVersion)
	assert.True(t, reflect.TypeOf(gocql.RoundRobinHostPolicy()) == reflect.TypeOf(cassandraConfig.loadBalancingPolicy))
	assert.Equal(t, int64(0), cassandraConfig.connectionTimeoutMillis)
	assert.Equal(t, int64(0), cassandraConfig.requestTimeoutMillis)
}

func TestExtractCassandraConfig_CorrectSettings(t *testing.T) {
	var config = map[string]any{
		"hosts":            []any{"0.0.0.0", "255.255.255.255"},
		"username":         "scylladb",
		"password":         "scylladb",
		"keyspace":         "scylladb",
		"protocol_version": 271.0,
		"load_balancing": map[string]any{
			"load_balancing_policy": "DCAwareRoundRobinPolicy",
			"local_dc":              "aws-us-west-2",
		},
		"connection_timeout_millis": 271.0,
		"request_timeout_millis":    271.0,
	}
	cassandraConfig, _ := extractCassandraConfig(config)

	assert.Equal(t, []string{"0.0.0.0", "255.255.255.255"}, cassandraConfig.hosts)
	assert.Equal(t, "scylladb", cassandraConfig.username)
	assert.Equal(t, "scylladb", cassandraConfig.password)
	assert.Equal(t, "scylladb", cassandraConfig.keyspace)
	assert.Equal(t, 271, cassandraConfig.protocolVersion)
	assert.True(t, reflect.TypeOf(gocql.DCAwareRoundRobinPolicy("aws-us-west-2")) == reflect.TypeOf(cassandraConfig.loadBalancingPolicy))
	assert.Equal(t, int64(271), cassandraConfig.connectionTimeoutMillis)
	assert.Equal(t, int64(271), cassandraConfig.requestTimeoutMillis)
}

func TestGetFqTableName(t *testing.T) {
	store := CassandraOnlineStore{
		project: "dummy_project",
		clusterConfigs: &gocql.ClusterConfig{
			Keyspace: "scylladb",
		},
	}

	fqTableName := store.getFqTableName("dummy_fv")
	assert.Equal(t, `"scylladb"."dummy_project_dummy_fv"`, fqTableName)
}

func TestGetSingleKeyCQLStatement(t *testing.T) {
	store := CassandraOnlineStore{}
	fqTableName := `"scylladb"."dummy_project_dummy_fv"`

	cqlStatement := store.getSingleKeyCQLStatement(fqTableName, []string{"feat1", "feat2"})
	assert.Equal(t,
		`SELECT "entity_key", "feature_name", "event_ts", "value" FROM "scylladb"."dummy_project_dummy_fv" WHERE "entity_key" = ? AND "feature_name" IN ('feat1','feat2')`,
		cqlStatement,
	)
}

func TestGetMultiKeyCQLStatement(t *testing.T) {
	store := CassandraOnlineStore{}
	fqTableName := `"scylladb"."dummy_project_dummy_fv"`

	cqlStatement := store.getMultiKeyCQLStatement(fqTableName, []string{"feat1", "feat2"}, 5)
	assert.Equal(t,
		`SELECT "entity_key", "feature_name", "event_ts", "value" FROM "scylladb"."dummy_project_dummy_fv" WHERE "entity_key" IN (?,?,?,?,?) AND "feature_name" IN ('feat1','feat2')`,
		cqlStatement,
	)
}

func TestOnlineRead_RejectsDifferentFeatureViewsInSameRead(t *testing.T) {
	store := CassandraOnlineStore{}
	_, err := store.OnlineRead(context.TODO(), nil, []string{"fv1", "fv2"}, []string{"feat1", "feat2"})
	assert.Error(t, err)
}
