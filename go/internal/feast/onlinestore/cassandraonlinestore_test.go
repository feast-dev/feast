package onlinestore

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/gocql/gocql"
	"github.com/stretchr/testify/assert"
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

	fqTableName := store.getFqTableName(store.clusterConfigs.Keyspace, store.project, "dummy_fv")
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

func TestGetFqTableNameWithinLimit(t *testing.T) {
	store := CassandraOnlineStore{
		project: "test_project",
	}

	keySpace := "test_keyspace"
	featureViewName := "test_feature_view"

	expectedTableName := `"test_keyspace"."test_project_test_feature_view"`
	actualTableName := store.getFqTableName(keySpace, store.project, featureViewName)

	assert.Equal(t, expectedTableName, actualTableName)
}

func TestGetFqTableNameExceedsLimit(t *testing.T) {
	store := CassandraOnlineStore{
		project: "test_project",
	}

	keySpace := "test_keyspace"
	featureViewName := "test_feature_view_with_a_very_long_name_exceeding_limit"

	expectedTableName := `"test_keyspace"."p6e72a69_test_feature_view_with_a_very__4d479508"`
	actualTableName := store.getFqTableName(keySpace, store.project, featureViewName)

	assert.Equal(t, expectedTableName, actualTableName)
}

func TestGetFqTableNameEdgeCase(t *testing.T) {
	store := CassandraOnlineStore{
		project: "test_project",
	}

	keySpace := "test_keyspace"
	featureViewName := strings.Repeat("a", 30)

	expectedTableName := `"test_keyspace"."test_project_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"`
	actualTableName := store.getFqTableName(keySpace, store.project, featureViewName)

	assert.Equal(t, expectedTableName, actualTableName)
}

func TestGetFqTableNameEdgeCaseExceedsLimit(t *testing.T) {
	store := CassandraOnlineStore{
		project: "test_project_edge",
	}

	keySpace := "test_keyspace"
	featureViewName := strings.Repeat("a", 31)

	expectedTableName := `"test_keyspace"."pfd98066_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaa_625ed0fd"`
	actualTableName := store.getFqTableName(keySpace, store.project, featureViewName)

	assert.Equal(t, expectedTableName, actualTableName)
}

func TestGetFqTableNameWithCache(t *testing.T) {
	store := CassandraOnlineStore{
		project: "test_project",
	}

	keySpace := "test_keyspace"
	featureViewName := "test_feature_view"

	// Pre-populate the cache
	tableName := fmt.Sprintf("%s_%s", store.project, featureViewName)
	expectedTableName := `"test_keyspace"."cached_table_name"`
	store.tableNameCache.Store(tableName, "cached_table_name")

	actualTableName := store.getFqTableName(keySpace, store.project, featureViewName)

	assert.Equal(t, expectedTableName, actualTableName)
}

func BenchmarkGetFqTableName(b *testing.B) {
	store := CassandraOnlineStore{
		project: "test_project",
	}
	keySpace := "test_keyspace"
	project := store.project
	featureViewNames := []string{"small_feature_view", "large_feature_view_large_feature_view_large_feature_view", "large_feature_view_large_feature_view_large_feature_view_large_feature_view_large_feature_view_large_feature_view_large_feature_view_large_feature_view_large_feature_view"}
	for i := 0; i < b.N; i++ {
		store.getFqTableName(keySpace, project, featureViewNames[i%3])
	}
}
