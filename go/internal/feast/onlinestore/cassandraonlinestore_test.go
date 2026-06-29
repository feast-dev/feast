//go:build !integration

package onlinestore

import (
	"encoding/hex"
	"fmt"
	"reflect"
	"testing"

	"github.com/feast-dev/feast/go/internal/feast/model"
	"github.com/feast-dev/feast/go/internal/feast/registry"
	"github.com/feast-dev/feast/go/internal/feast/utils"
	"github.com/feast-dev/feast/go/protos/feast/core"
	"github.com/feast-dev/feast/go/protos/feast/serving"
	"github.com/feast-dev/feast/go/protos/feast/types"
	"github.com/gocql/gocql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
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

	tableNameVersion := 1

	fqTableName, _ := store.getFqTableName(store.clusterConfigs.Keyspace, store.project, "dummy_fv", tableNameVersion)
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

func TestGetFqTableName_Version1(t *testing.T) {
	store := CassandraOnlineStore{
		project: "test_project",
	}

	keySpace := "test_keyspace"
	featureViewName := "test_feature_view"
	tableNameVersion := 1

	expectedTableName := `"test_keyspace"."test_project_test_feature_view"`
	actualTableName, err := store.getFqTableName(keySpace, store.project, featureViewName, tableNameVersion)

	assert.NoError(t, err)
	assert.Equal(t, expectedTableName, actualTableName)
}

func TestGetFqTableName_Version2_WithinLimit(t *testing.T) {
	store := CassandraOnlineStore{
		project: "test_project",
	}

	keySpace := "test_keyspace"
	featureViewName := "test_feature_view"
	tableNameVersion := 2

	expectedTableName := `"test_keyspace"."test_project_test_feature_view"`
	actualTableName, err := store.getFqTableName(keySpace, store.project, featureViewName, tableNameVersion)

	assert.NoError(t, err)
	assert.Equal(t, expectedTableName, actualTableName)
}

func TestGetFqTableName_Version2_ExceedsLimit(t *testing.T) {
	store := CassandraOnlineStore{
		project: "test_project",
	}

	keySpace := "test_keyspace"
	featureViewName := "test_feature_view_with_a_very_long_name_exceeding_limit"
	tableNameVersion := 2

	expectedTableName := `"test_keyspace"."test__29UZUpJQRijDZsYzl_test__5Ur8Mv5QutEG23Cp2C"`
	actualTableName, err := store.getFqTableName(keySpace, store.project, featureViewName, tableNameVersion)

	assert.NoError(t, err)
	assert.Equal(t, expectedTableName, actualTableName)
}

func TestGetFqTableName_InvalidVersion(t *testing.T) {
	store := CassandraOnlineStore{
		project: "test_project",
	}

	keySpace := "test_keyspace"
	featureViewName := "test_feature_view"
	tableNameVersion := 3

	_, err := store.getFqTableName(keySpace, store.project, featureViewName, tableNameVersion)
	assert.Error(t, err)
	assert.Equal(t, "unknown table name format version: 3", err.Error())
}

func TestGetFqTableName_WithCache(t *testing.T) {
	store := CassandraOnlineStore{
		project: "test_project",
	}

	keySpace := "test_keyspace"
	featureViewName := "test_feature_view"

	// Pre-populate the cache
	tableName := fmt.Sprintf("%s_%s", store.project, featureViewName)
	expectedTableName := `"test_keyspace"."cached_table_name"`
	store.tableNameCache.Store(tableName, "cached_table_name")

	actualTableName, err := store.getFqTableName(keySpace, store.project, featureViewName, 1)

	assert.NoError(t, err)
	assert.Equal(t, expectedTableName, actualTableName)
}

func TestGetFqTableName_EmptyCache(t *testing.T) {
	store := CassandraOnlineStore{
		project: "test_project",
	}

	keySpace := "test_keyspace"
	featureViewName := "test_feature_view"
	tableNameVersion := 1

	expectedTableName := `"test_keyspace"."test_project_test_feature_view"`
	actualTableName, err := store.getFqTableName(keySpace, store.project, featureViewName, tableNameVersion)

	assert.NoError(t, err)
	assert.Equal(t, expectedTableName, actualTableName)

	// Verify that the table name is cached
	cachedValue, found := store.tableNameCache.Load(fmt.Sprintf("%s_%s", store.project, featureViewName))
	assert.True(t, found)
	assert.Equal(t, "test_project_test_feature_view", cachedValue)
}
func BenchmarkGetFqTableName(b *testing.B) {
	store := CassandraOnlineStore{
		project: "test_project",
	}
	keySpace := "test_keyspace"
	project := store.project
	tableNameVersion := 2
	featureViewNames := []string{"small_feature_view", "large_feature_view_large_feature_view_large_feature_view", "large_feature_view_large_feature_view_large_feature_view_large_feature_view_large_feature_view_large_feature_view_large_feature_view_large_feature_view_large_feature_view"}
	for i := 0; i < b.N; i++ {
		store.getFqTableName(keySpace, project, featureViewNames[i%3], tableNameVersion)
	}
}

func TestCreateBatches(t *testing.T) {
	c := &CassandraOnlineStore{KeyBatchSize: 3}
	keys := []any{0, 1, 2, 3, 4, 5, 6, 7}

	batches := c.createBatches(keys)
	if len(batches) != 3 {
		t.Errorf("Expected 3 batches, got %d", len(batches))
	}

	if len(batches[2]) != 2 {
		t.Errorf("Last batch should be size 2, got %d", len(batches[2]))
	}

	var flattened []any
	for _, batch := range batches {
		flattened = append(flattened, batch...)
	}

	for i, key := range keys {
		if flattened[i] != key {
			t.Errorf("Position %d: got %v, want %v", i, flattened[i], key)
		}
	}
}

func BenchmarkCreateBatches(b *testing.B) {
	c := &CassandraOnlineStore{KeyBatchSize: 100}
	keys := make([]any, 1000)
	for i := 0; i < 1000; i++ {
		keys[i] = i
	}

	b.ResetTimer() // capture only create batch time.
	for i := 0; i < b.N; i++ {
		c.createBatches(keys)
	}
}

func TestCassandraOnlineStore_buildRangeQueryCQL_singleFilter(t *testing.T) {
	store := CassandraOnlineStore{}
	fqTableName := `"scylladb"."dummy_project_dummy_fv"`
	sortFilter1 := model.SortKeyFilter{
		SortKeyName:    "sort1",
		RangeStart:     4,
		RangeEnd:       12,
		StartInclusive: true,
		EndInclusive:   true,
		Order:          &model.SortOrder{Order: core.SortOrder_ASC},
	}

	cqlStatement, params := store.buildRangeQueryCQL(fqTableName, []string{"feat1", "feat2"}, 1, []*model.SortKeyFilter{&sortFilter1}, 5, false)
	assert.Equal(t,
		`SELECT entity_key, event_ts, feat1, feat2 FROM "scylladb"."dummy_project_dummy_fv" WHERE entity_key = ? AND sort1 >= ? AND sort1 <= ? PER PARTITION LIMIT ?`,
		cqlStatement,
	)
	assert.ElementsMatch(t, []interface{}{4, 12, int32(5)}, params)
}

func TestCassandraOnlineStore_buildRangeQueryCQL_withoutLimit(t *testing.T) {
	store := CassandraOnlineStore{}
	fqTableName := `"scylladb"."dummy_project_dummy_fv"`
	sortFilter1 := model.SortKeyFilter{
		SortKeyName:  "sort1",
		RangeEnd:     12,
		EndInclusive: true,
		Order:        &model.SortOrder{Order: core.SortOrder_ASC},
	}

	cqlStatement, params := store.buildRangeQueryCQL(fqTableName, []string{"feat1", "feat2"}, 1, []*model.SortKeyFilter{&sortFilter1}, 0, false)
	assert.Equal(t,
		`SELECT entity_key, event_ts, feat1, feat2 FROM "scylladb"."dummy_project_dummy_fv" WHERE entity_key = ? AND sort1 <= ?`,
		cqlStatement,
	)
	assert.ElementsMatch(t, []interface{}{12}, params)
}

func TestCassandraOnlineStore_buildRangeQueryCQL_multipleFilters(t *testing.T) {
	store := CassandraOnlineStore{}
	fqTableName := `"scylladb"."dummy_project_dummy_fv"`
	sortFilter1 := model.SortKeyFilter{
		SortKeyName: "sort1",
		Equals:      4,
		Order:       &model.SortOrder{Order: core.SortOrder_ASC},
	}
	sortFilter2 := model.SortKeyFilter{
		SortKeyName:    "sort2",
		RangeStart:     10,
		StartInclusive: true,
		Order:          &model.SortOrder{Order: core.SortOrder_DESC},
	}

	cqlStatement, params := store.buildRangeQueryCQL(fqTableName, []string{"feat1", "feat2"}, 1, []*model.SortKeyFilter{&sortFilter1, &sortFilter2}, 5, false)
	assert.Equal(t,
		`SELECT entity_key, event_ts, feat1, feat2 FROM "scylladb"."dummy_project_dummy_fv" WHERE entity_key = ? AND sort1 = ? AND sort2 >= ? PER PARTITION LIMIT ?`,
		cqlStatement,
	)
	assert.ElementsMatch(t, []interface{}{4, 10, int32(5)}, params)
}

func TestCassandraOnlineStore_buildRangeQueryCQL_multipleFiltersWithMixOfRanges(t *testing.T) {
	store := CassandraOnlineStore{}
	fqTableName := `"scylladb"."dummy_project_dummy_fv"`
	sortFilter1 := model.SortKeyFilter{
		SortKeyName:    "sort1",
		RangeStart:     4,
		RangeEnd:       12,
		StartInclusive: true,
		EndInclusive:   false,
		Order:          &model.SortOrder{Order: core.SortOrder_ASC},
	}
	sortFilter2 := model.SortKeyFilter{
		SortKeyName:    "sort2",
		RangeStart:     10,
		RangeEnd:       20,
		StartInclusive: false,
		EndInclusive:   true,
		Order:          &model.SortOrder{Order: core.SortOrder_DESC},
	}
	sortFilter3 := model.SortKeyFilter{
		SortKeyName: "sort3",
		Order:       &model.SortOrder{Order: core.SortOrder_ASC},
	}

	cqlStatement, params := store.buildRangeQueryCQL(fqTableName, []string{"feat1", "feat2"}, 1, []*model.SortKeyFilter{&sortFilter1, &sortFilter2, &sortFilter3}, 5, false)
	assert.Equal(t,
		`SELECT entity_key, event_ts, feat1, feat2 FROM "scylladb"."dummy_project_dummy_fv" WHERE entity_key = ? AND sort1 >= ? AND sort1 < ? AND sort2 > ? AND sort2 <= ? PER PARTITION LIMIT ?`,
		cqlStatement,
	)
	assert.ElementsMatch(t, []interface{}{4, 12, 10, 20, int32(5)}, params)
}

func TestCassandraOnlineStore_buildRangeQueryCQL_noFilters(t *testing.T) {
	store := CassandraOnlineStore{}
	fqTableName := `"scylladb"."dummy_project_dummy_fv"`

	cqlStatement, params := store.buildRangeQueryCQL(fqTableName, []string{"feat1", "feat2"}, 1, []*model.SortKeyFilter{}, 5, false)
	assert.Equal(t,
		`SELECT entity_key, event_ts, feat1, feat2 FROM "scylladb"."dummy_project_dummy_fv" WHERE entity_key = ? PER PARTITION LIMIT ?`,
		cqlStatement,
	)
	assert.ElementsMatch(t, []interface{}{int32(5)}, params)
}

func TestCassandraOnlineStore_buildRangeQueryCQL_reverseSortOrder(t *testing.T) {
	store := CassandraOnlineStore{}
	fqTableName := `"scylladb"."dummy_project_dummy_fv"`
	sortFilter := model.SortKeyFilter{
		SortKeyName:    "sort1",
		RangeStart:     5,
		StartInclusive: true,
		Order:          &model.SortOrder{Order: core.SortOrder_DESC},
	}

	cqlStatement, params := store.buildRangeQueryCQL(
		fqTableName,
		[]string{"feat1", "feat2"},
		1,
		[]*model.SortKeyFilter{&sortFilter},
		7, // limit 7
		true,
	)

	assert.Equal(t,
		`SELECT entity_key, event_ts, feat1, feat2 FROM "scylladb"."dummy_project_dummy_fv" WHERE entity_key = ? AND sort1 >= ? ORDER BY sort1 DESC PER PARTITION LIMIT ?`,
		cqlStatement,
	)

	assert.ElementsMatch(t, []interface{}{5, int32(7)}, params)
}

func TestCassandraOnlineStore_buildRangeQueryCQL_batchedKeysWithoutFilters(t *testing.T) {
	store := CassandraOnlineStore{}
	fqTableName := `"scylladb"."dummy_project_dummy_fv"`

	cqlStatement, params := store.buildRangeQueryCQL(
		fqTableName,
		[]string{"feat1", "feat2"},
		2,
		[]*model.SortKeyFilter{},
		10,
		true,
	)

	assert.Equal(t,
		`SELECT entity_key, event_ts, feat1, feat2 FROM "scylladb"."dummy_project_dummy_fv" WHERE entity_key IN (?, ?) PER PARTITION LIMIT ?`,
		cqlStatement,
	)

	assert.ElementsMatch(t, []interface{}{int32(10)}, params)
}

func TestCassandraOnlineStore_buildRangeQueryCQL_orderNil_skipsOrderBy(t *testing.T) {
	store := CassandraOnlineStore{}
	fqTableName := `"scylladb"."dummy_project_dummy_fv"`

	sortFilter := model.SortKeyFilter{
		SortKeyName: "sort1",
		Equals:      42,
		Order:       nil,
	}

	cql, params := store.buildRangeQueryCQL(
		fqTableName,
		[]string{"feat1"},
		// one entity key is eligible for unbatched query
		1,
		[]*model.SortKeyFilter{&sortFilter},
		0,
		false,
	)

	expectedCQL :=
		`SELECT entity_key, event_ts, feat1 ` +
			`FROM "scylladb"."dummy_project_dummy_fv" ` +
			`WHERE entity_key = ? AND sort1 = ?`

	assert.Equal(t, expectedCQL, cql)
	assert.ElementsMatch(t, []interface{}{42}, params)
	assert.NotContains(t, cql, "ORDER BY", "ORDER BY should be omitted when all SortKeyFilters have Order == nil")
}

func TestBuildRangeQueryCQL_UsesLowercaseUnquotedIdentifiers(t *testing.T) {
	store := CassandraOnlineStore{}
	fqTableName := `"scylladb"."dummy_project_dummy_fv"`

	cql, _ := store.buildRangeQueryCQL(
		fqTableName,
		[]string{"featureX", "FeatureY"},
		2,
		nil,
		0,
		false,
	)
	// Feature columns must appear lowercase and unquoted in projection.
	assert.Contains(t, cql, "featurex")
	assert.Contains(t, cql, "featurey")
	assert.NotContains(t, cql, `"featureX"`)
	assert.NotContains(t, cql, `"FeatureY"`)
	// entity_key and event_ts also unquoted.
	assert.Contains(t, cql, "entity_key")
	assert.NotContains(t, cql, `"entity_key"`)
}

func TestCanonicalColumnName(t *testing.T) {
	assert.Equal(t, "featurex", canonicalColumnName("featureX"))
	assert.Equal(t, "feature", canonicalColumnName("FEATURE"))
	assert.Equal(t, "already_lower", canonicalColumnName("already_lower"))
	assert.Equal(t, "", canonicalColumnName(""))
}

func mustMarshalValueProto(t *testing.T, val *types.Value) []byte {
	t.Helper()
	b, err := proto.Marshal(val)
	if err != nil {
		t.Fatalf("failed to marshal value proto: %v", err)
	}
	return b
}

func TestResolveFeatureValue_MixedCaseNonSortKey(t *testing.T) {
	// Regression: gocql MapScan returns row keys in the case Cassandra stored
	// them (lowercase, since the Python writer emits unquoted DDL). FV feature
	// names from the registry preserve their original case. The caller must
	// canonicalize the FV name before passing it here.
	testVal := &types.Value{Val: &types.Value_Int64Val{Int64Val: 99}}
	serialized := mustMarshalValueProto(t, testVal)

	readValues := map[string]interface{}{
		"sumclicksxdestinationgeoid": serialized,
	}

	val, status, err := resolveFeatureValue(readValues, canonicalColumnName("sumClicksXDestinationGeoId"), false)
	assert.NoError(t, err)
	assert.Equal(t, serving.FieldStatus_PRESENT, status)
	assert.NotNil(t, val)
	assert.Equal(t, int64(99), val.(*types.Value).GetInt64Val())
}

func TestResolveFeatureValue_MixedCaseSortKey(t *testing.T) {
	readValues := map[string]interface{}{
		"eventoriginationtimestamp": int64(1714500000),
	}

	val, status, err := resolveFeatureValue(readValues, canonicalColumnName("EventOriginationTimestamp"), true)
	assert.NoError(t, err)
	assert.Equal(t, serving.FieldStatus_PRESENT, status)
	assert.Equal(t, int64(1714500000), val)
}

func TestResolveFeatureValue_SortKeyNullValue(t *testing.T) {
	readValues := map[string]interface{}{
		"sortkey": nil,
	}

	val, status, err := resolveFeatureValue(readValues, canonicalColumnName("SortKey"), true)
	assert.NoError(t, err)
	assert.Equal(t, serving.FieldStatus_NULL_VALUE, status)
	assert.Nil(t, val)
}

func TestResolveFeatureValue_FeatureMissingFromRow(t *testing.T) {
	readValues := map[string]interface{}{
		"entity_key": "abc",
	}

	val, status, err := resolveFeatureValue(readValues, canonicalColumnName("MissingFeature"), false)
	assert.NoError(t, err)
	assert.Equal(t, serving.FieldStatus_NOT_FOUND, status)
	assert.Nil(t, val)
}

func TestResolveFeatureValue_SortKeyMissingFromRow(t *testing.T) {
	readValues := map[string]interface{}{}

	val, status, err := resolveFeatureValue(readValues, canonicalColumnName("SortKey"), true)
	assert.NoError(t, err)
	assert.Equal(t, serving.FieldStatus_NOT_FOUND, status)
	assert.Nil(t, val)
}

func TestResolveFeatureValue_AlreadyLowercaseFeature(t *testing.T) {
	testVal := &types.Value{Val: &types.Value_StringVal{StringVal: "hello"}}
	serialized := mustMarshalValueProto(t, testVal)

	readValues := map[string]interface{}{
		"already_lower": serialized,
	}

	val, status, err := resolveFeatureValue(readValues, canonicalColumnName("already_lower"), false)
	assert.NoError(t, err)
	assert.Equal(t, serving.FieldStatus_PRESENT, status)
	assert.Equal(t, "hello", val.(*types.Value).GetStringVal())
}

func sampleEntityKey() *types.EntityKey {
	return &types.EntityKey{
		JoinKeys: []string{"user_id"},
		EntityValues: []*types.Value{
			{Val: &types.Value_Int64Val{Int64Val: 42}},
		},
	}
}

func serializedHex(t *testing.T, key *types.EntityKey, version int64) string {
	t.Helper()
	b, err := utils.SerializeEntityKey(key, version)
	require.NoError(t, err)
	return hex.EncodeToString(*b)
}

func TestBuildCassandraEntityKeys_UsesConfiguredVersion(t *testing.T) {
	key := sampleEntityKey()

	for _, tc := range []struct {
		name    string
		version int64
	}{
		{"version2", 2},
		{"version3", 3},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := &CassandraOnlineStore{
				config: &registry.RepoConfig{
					EntityKeySerializationVersion: tc.version,
				},
			}

			cassandraKeys, keyToIdx, err := store.buildCassandraEntityKeys([]*types.EntityKey{key})
			require.NoError(t, err)
			require.Len(t, cassandraKeys, 1)

			gotHex := cassandraKeys[0].(string)
			wantHex := serializedHex(t, key, tc.version)

			assert.Equal(t, wantHex, gotHex,
				"hex key for version %d should match utils.SerializeEntityKey output", tc.version)
			assert.Equal(t, 0, keyToIdx[gotHex])
		})
	}

	storeV2 := &CassandraOnlineStore{config: &registry.RepoConfig{EntityKeySerializationVersion: 2}}
	storeV3 := &CassandraOnlineStore{config: &registry.RepoConfig{EntityKeySerializationVersion: 3}}
	keysV2, _, _ := storeV2.buildCassandraEntityKeys([]*types.EntityKey{key})
	keysV3, _, _ := storeV3.buildCassandraEntityKeys([]*types.EntityKey{key})
	assert.NotEqual(t, keysV2[0], keysV3[0],
		"v2 and v3 serialized keys must differ for the same entity key")
}

func TestBuildCassandraEntityKeys_ZeroVersionDefaultsToV3(t *testing.T) {
	key := sampleEntityKey()

	store := &CassandraOnlineStore{
		config: &registry.RepoConfig{EntityKeySerializationVersion: 0},
	}
	keys, _, err := store.buildCassandraEntityKeys([]*types.EntityKey{key})
	require.NoError(t, err)

	wantHex := serializedHex(t, key, 3)
	assert.Equal(t, wantHex, keys[0].(string), "zero version should produce v3 keys")
}

func TestResolvedEntityKeySerializationVersion(t *testing.T) {
	assert.Equal(t, int64(3), (&CassandraOnlineStore{config: &registry.RepoConfig{EntityKeySerializationVersion: 0}}).resolvedEntityKeySerializationVersion())
	assert.Equal(t, int64(2), (&CassandraOnlineStore{config: &registry.RepoConfig{EntityKeySerializationVersion: 2}}).resolvedEntityKeySerializationVersion())
	assert.Equal(t, int64(3), (&CassandraOnlineStore{config: &registry.RepoConfig{EntityKeySerializationVersion: 3}}).resolvedEntityKeySerializationVersion())
	assert.Equal(t, int64(3), (&CassandraOnlineStore{config: nil}).resolvedEntityKeySerializationVersion(), "nil config should default to v3 without panicking")
}

func TestWarnPotentialVersionMismatch_DeduplicatesPerRequest(t *testing.T) {
	store := &CassandraOnlineStore{
		config: &registry.RepoConfig{EntityKeySerializationVersion: 3},
	}

	const view = "my_feature_view"

	store.warnPotentialVersionMismatch([]string{view}, 5)
	_, warned := store.versionMismatchWarned.Load(view)
	assert.True(t, warned, "view should be recorded after first call")

	store.warnPotentialVersionMismatch([]string{view}, 5)
	count := 0
	store.versionMismatchWarned.Range(func(_, _ any) bool { count++; return true })
	assert.Equal(t, 1, count, "only one entry should exist for the view after repeated calls")

	const otherView = "other_feature_view"
	store.warnPotentialVersionMismatch([]string{otherView}, 3)
	count = 0
	store.versionMismatchWarned.Range(func(_, _ any) bool { count++; return true })
	assert.Equal(t, 2, count, "each distinct request should have its own warning entry")
}

func TestWarnPotentialVersionMismatch_MultiViewDedupIsOrderIndependent(t *testing.T) {
	store := &CassandraOnlineStore{
		config: &registry.RepoConfig{EntityKeySerializationVersion: 3},
	}

	store.warnPotentialVersionMismatch([]string{"view_a", "view_b"}, 4)
	store.warnPotentialVersionMismatch([]string{"view_b", "view_a"}, 4)

	count := 0
	store.versionMismatchWarned.Range(func(_, _ any) bool { count++; return true })
	assert.Equal(t, 1, count, "same set of views in any order should dedupe to one entry")
}

func TestWarnPotentialVersionMismatch_EmptyViewsIsNoop(t *testing.T) {
	store := &CassandraOnlineStore{
		config: &registry.RepoConfig{EntityKeySerializationVersion: 3},
	}

	store.warnPotentialVersionMismatch(nil, 5)
	store.warnPotentialVersionMismatch([]string{}, 5)

	count := 0
	store.versionMismatchWarned.Range(func(_, _ any) bool { count++; return true })
	assert.Equal(t, 0, count, "no warning entry should be recorded for an empty view set")
}
