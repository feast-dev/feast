package feast

import (
	"context"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"

	"github.com/feast-dev/feast/go/internal/feast/model"
	"github.com/feast-dev/feast/go/internal/feast/onlineserving"
	"github.com/feast-dev/feast/go/internal/feast/onlinestore"
	"github.com/feast-dev/feast/go/internal/feast/registry"
	"github.com/feast-dev/feast/go/internal/test"
	"github.com/feast-dev/feast/go/protos/feast/core"
	"github.com/feast-dev/feast/go/protos/feast/serving"
	"github.com/feast-dev/feast/go/protos/feast/types"
)

var featureRepoBasePath string
var featureRepoRegistryFile string

func TestMain(m *testing.M) {
	// Get the file path of this source file, regardless of the working directory
	_, filename, _, ok := runtime.Caller(0)
	if !ok {
		log.Print("couldn't find file path of the test file")
		os.Exit(1)
	}
	featureRepoBasePath = filepath.Join(filename, "..", "..", "test")
	featureRepoRegistryFile = filepath.Join(featureRepoBasePath, "feature_repo", "data", "registry.db")
	if err := test.SetupInitializedRepo(featureRepoBasePath); err != nil {
		log.Print("Could not initialize test repo: ", err)
		os.Exit(1)
	}
	os.Exit(m.Run())
}

func TestNewFeatureStore(t *testing.T) {
	tests := []struct {
		name                  string
		config                *registry.RepoConfig
		expectOnlineStoreType interface{}
		errMessage            string
	}{
		{
			name: "valid config",
			config: &registry.RepoConfig{
				Project: "feature_repo",
				Registry: map[string]interface{}{
					"path": featureRepoRegistryFile,
				},
				Provider: "local",
				OnlineStore: map[string]interface{}{
					"type": "redis",
				},
			},
			expectOnlineStoreType: &onlinestore.RedisOnlineStore{},
		},
		{
			name: "valid config with transformation service endpoint",
			config: &registry.RepoConfig{
				Project: "feature_repo",
				Registry: map[string]interface{}{
					"path": featureRepoRegistryFile,
				},
				Provider: "local",
				OnlineStore: map[string]interface{}{
					"type": "redis",
				},
				FeatureServer: map[string]interface{}{
					"transformation_service_endpoint": "localhost:50051",
				},
			},
			expectOnlineStoreType: &onlinestore.RedisOnlineStore{},
		},
		{
			name: "invalid online store config",
			config: &registry.RepoConfig{
				Project: "feature_repo",
				Registry: map[string]interface{}{
					"path": featureRepoRegistryFile,
				},
				Provider: "local",
				OnlineStore: map[string]interface{}{
					"type": "invalid_store",
				},
			},
			errMessage: "invalid_store online store type is currently not supported",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := NewFeatureStore(test.config, nil)
			if test.errMessage != "" {
				assert.Nil(t, got)
				require.Error(t, err)
				assert.ErrorContains(t, err, test.errMessage)

			} else {
				require.NoError(t, err)
				assert.NotNil(t, got)
				assert.IsType(t, test.expectOnlineStoreType, got.onlineStore)
			}
		})
	}

}

type MockRedis struct {
	mock.Mock
}

func (m *MockRedis) Destruct() {}
func (m *MockRedis) OnlineRead(ctx context.Context, entityKeys []*types.EntityKey, featureViewNames []string, featureNames []string) ([][]onlinestore.FeatureData, error) {
	args := m.Called(ctx, entityKeys, featureViewNames, featureNames)
	var fd [][]onlinestore.FeatureData
	if args.Get(0) != nil {
		fd = args.Get(0).([][]onlinestore.FeatureData)
	}
	return fd, args.Error(1)
}

func TestGetOnlineFeatures(t *testing.T) {
	tests := []struct {
		name   string
		config *registry.RepoConfig
		fn     func(*testing.T, *FeatureStore)
	}{
		{
			name: "redis with simple features",
			config: &registry.RepoConfig{
				Project: "feature_repo",
				Registry: map[string]interface{}{
					"path": featureRepoRegistryFile,
				},
				Provider: "local",
				OnlineStore: map[string]interface{}{
					"type":              "redis",
					"connection_string": "localhost:6379",
				},
			},
			fn: testRedisSimpleFeatures,
		},
		{
			name: "redis with On-demand feature views, no transformation service endpoint",
			config: &registry.RepoConfig{
				Project: "feature_repo",
				Registry: map[string]interface{}{
					"path": featureRepoRegistryFile,
				},
				Provider: "local",
				OnlineStore: map[string]interface{}{
					"type":              "redis",
					"connection_string": "localhost:6379",
				},
			},
			fn: testRedisODFVNoTransformationService,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {

			fs, err := NewFeatureStore(test.config, nil)
			require.Nil(t, err)
			fs.onlineStore = new(MockRedis)
			test.fn(t, fs)
		})

	}
}

func testRedisSimpleFeatures(t *testing.T, fs *FeatureStore) {

	featureNames := []string{"driver_hourly_stats:conv_rate",
		"driver_hourly_stats:acc_rate",
		"driver_hourly_stats:avg_daily_trips",
	}
	entities := map[string]*types.RepeatedValue{"driver_id": {Val: []*types.Value{{Val: &types.Value_Int64Val{Int64Val: 1001}},
		{Val: &types.Value_Int64Val{Int64Val: 1002}},
	}}}

	results := [][]onlinestore.FeatureData{
		{
			{
				Reference: serving.FeatureReferenceV2{FeatureViewName: "driver_hourly_stats", FeatureName: "conv_rate"},
				Value:     types.Value{Val: &types.Value_FloatVal{FloatVal: 12.0}},
			},
			{
				Reference: serving.FeatureReferenceV2{FeatureViewName: "driver_hourly_stats", FeatureName: "acc_rate"},
				Value:     types.Value{Val: &types.Value_FloatVal{FloatVal: 1.0}},
			},
			{
				Reference: serving.FeatureReferenceV2{FeatureViewName: "driver_hourly_stats", FeatureName: "avg_daily_trips"},
				Value:     types.Value{Val: &types.Value_Int64Val{Int64Val: 100}},
			},
		},
		{

			{
				Reference: serving.FeatureReferenceV2{FeatureViewName: "driver_hourly_stats", FeatureName: "conv_rate"},
				Value:     types.Value{Val: &types.Value_FloatVal{FloatVal: 24.0}},
			},
			{
				Reference: serving.FeatureReferenceV2{FeatureViewName: "driver_hourly_stats", FeatureName: "acc_rate"},
				Value:     types.Value{Val: &types.Value_FloatVal{FloatVal: 2.0}},
			},
			{
				Reference: serving.FeatureReferenceV2{FeatureViewName: "driver_hourly_stats", FeatureName: "avg_daily_trips"},
				Value:     types.Value{Val: &types.Value_Int64Val{Int64Val: 130}},
			},
		},
	}
	ctx := context.Background()
	mr := fs.onlineStore.(*MockRedis)
	mr.On("OnlineRead", ctx, mock.Anything, mock.Anything, mock.Anything).Return(results, nil)
	response, err := fs.GetOnlineFeatures(ctx, featureNames, nil, entities, map[string]*types.RepeatedValue{}, true)
	require.Nil(t, err)
	assert.Len(t, response, 4) // 3 Features + 1 entity = 4 columns (feature vectors) in response
}

func testRedisODFVNoTransformationService(t *testing.T, fs *FeatureStore) {
	featureNames := []string{"driver_hourly_stats:conv_rate",
		"driver_hourly_stats:acc_rate",
		"driver_hourly_stats:avg_daily_trips",
		"transformed_conv_rate:conv_rate_plus_val1",
	}
	entities := map[string]*types.RepeatedValue{"driver_id": {Val: []*types.Value{{Val: &types.Value_Int64Val{Int64Val: 1001}},
		{Val: &types.Value_Int64Val{Int64Val: 1002}},
		{Val: &types.Value_Int64Val{Int64Val: 1003}}}},
	}

	ctx := context.Background()
	mr := fs.onlineStore.(*MockRedis)
	mr.On("OnlineRead", ctx, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
	response, err := fs.GetOnlineFeatures(ctx, featureNames, nil, entities, map[string]*types.RepeatedValue{}, true)
	assert.Nil(t, response)
	assert.ErrorAs(t, err, &FeastTransformationServiceNotConfigured{})

}

// entityAwareFakeStore implements both OnlineStore and EntityAwareOnlineStore and records
// which read method the feature store called, and with which entity names.
type entityAwareFakeStore struct {
	plainReads        int
	entityNamesByView []map[string][]string
}

var _ onlinestore.OnlineStore = (*entityAwareFakeStore)(nil)
var _ onlinestore.EntityAwareOnlineStore = (*entityAwareFakeStore)(nil)

func (s *entityAwareFakeStore) Destruct() {}

func (s *entityAwareFakeStore) OnlineRead(_ context.Context, entityKeys []*types.EntityKey,
	_ []string, _ []string) ([][]onlinestore.FeatureData, error) {
	s.plainReads++
	return make([][]onlinestore.FeatureData, len(entityKeys)), nil
}

func (s *entityAwareFakeStore) OnlineReadWithEntities(_ context.Context, entityKeys []*types.EntityKey,
	_ []string, _ []string, entityNamesByView map[string][]string) ([][]onlinestore.FeatureData, error) {
	s.entityNamesByView = append(s.entityNamesByView, entityNamesByView)
	return make([][]onlinestore.FeatureData, len(entityKeys)), nil
}

func viewAndRefs(name string, entityNames []string, featureRefs ...string) *onlineserving.FeatureViewAndRefs {
	return &onlineserving.FeatureViewAndRefs{
		View:        &model.FeatureView{Base: model.NewBaseFeatureView(name, nil), EntityNames: entityNames},
		FeatureRefs: featureRefs,
	}
}

func int64EntityKey(joinKey string, value int64) *types.EntityKey {
	return &types.EntityKey{
		JoinKeys:     []string{joinKey},
		EntityValues: []*types.Value{{Val: &types.Value_Int64Val{Int64Val: value}}},
	}
}

func TestReadFromOnlineStoreUsesEntityAwareStore(t *testing.T) {
	store := &entityAwareFakeStore{}
	fs := &FeatureStore{onlineStore: store}
	// A second, aliased projection of fvA keeps the base name and the entities; the map keys on Base.Name.
	aliasedA := viewAndRefs("fvA", []string{"subscriber"}, "f3")
	aliasedA.View.Base.Projection = &model.FeatureViewProjection{Name: "fvA", NameAlias: "fvA_alias"}
	views := []*onlineserving.FeatureViewAndRefs{
		viewAndRefs("fvA", []string{"subscriber"}, "f1"),
		viewAndRefs("fvB", []string{"household"}, "f2"),
		aliasedA,
		// An entityless view carries the dummy entity, which the store needs to find its table.
		viewAndRefs("fvNoEntity", []string{model.DUMMY_ENTITY_NAME}, "f4"),
	}
	keys := []*types.EntityKey{int64EntityKey("user_profile_id", 1)}

	rows, err := fs.readFromOnlineStore(context.Background(), keys,
		[]string{"fvA", "fvB", "fvA", "fvNoEntity"}, []string{"f1", "f2", "f3", "f4"}, views)

	require.NoError(t, err)
	assert.Len(t, rows, 1)
	assert.Zero(t, store.plainReads)
	assert.Equal(t, []map[string][]string{
		{"fvA": {"subscriber"}, "fvB": {"household"}, "fvNoEntity": {"__dummy"}},
	}, store.entityNamesByView)
}

func TestReadFromOnlineStorePlainStoreUnchanged(t *testing.T) {
	store := new(MockRedis)
	fs := &FeatureStore{onlineStore: store}
	views := []*onlineserving.FeatureViewAndRefs{viewAndRefs("fvA", []string{"subscriber"}, "f1")}
	keys := []*types.EntityKey{int64EntityKey("user_profile_id", 1)}
	want := [][]onlinestore.FeatureData{{{
		Reference: serving.FeatureReferenceV2{FeatureViewName: "fvA", FeatureName: "f1"},
		Value:     types.Value{Val: &types.Value_Int64Val{Int64Val: 7}},
	}}}
	store.On("OnlineRead", mock.Anything, keys, []string{"fvA"}, []string{"f1"}).Return(want, nil)

	rows, err := fs.readFromOnlineStore(context.Background(), keys, []string{"fvA"}, []string{"f1"}, views)

	require.NoError(t, err)
	assert.Equal(t, want, rows)
	store.AssertExpectations(t)
}

// writeRegistryFile serialises a registry proto to a file the file registry store can load.
func writeRegistryFile(t *testing.T, registryProto *core.Registry) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "registry.db")
	data, err := proto.Marshal(registryProto)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(path, data, 0o644))
	return path
}

func TestStreamViewWinsForEntityAwareStore(t *testing.T) {
	const project = "stream_wins"
	features := []*core.FeatureSpecV2{{Name: "f1", ValueType: types.ValueType_INT64}}
	registryPath := writeRegistryFile(t, &core.Registry{
		RegistrySchemaVersion: registry.REGISTRY_SCHEMA_VERSION,
		Entities: []*core.Entity{
			{Spec: &core.EntitySpecV2{Name: "subscriber", Project: project, JoinKey: "user_profile_id"}},
			{Spec: &core.EntitySpecV2{Name: "household", Project: project, JoinKey: "household_id"}},
		},
		FeatureViews: []*core.FeatureView{{Spec: &core.FeatureViewSpec{
			Name: "fv", Project: project, Entities: []string{"household"}, Features: features,
			Ttl: &durationpb.Duration{},
		}}},
		StreamFeatureViews: []*core.StreamFeatureView{{Spec: &core.StreamFeatureViewSpec{
			Name: "fv", Project: project, Entities: []string{"subscriber"}, Features: features,
			Ttl: &durationpb.Duration{},
		}}},
	})
	fs, err := NewFeatureStore(&registry.RepoConfig{
		Project: project,
		// A zero TTL loads the registry once with no periodic refresh.
		Registry:    map[string]interface{}{"path": registryPath, "cache_ttl_seconds": 0},
		Provider:    "local",
		OnlineStore: map[string]interface{}{"type": "redis", "connection_string": "localhost:6379"},
	}, nil)
	require.NoError(t, err)
	fs.onlineStore.Destruct()
	store := &entityAwareFakeStore{}
	fs.onlineStore = store
	entities := map[string]*types.RepeatedValue{
		"user_profile_id": {Val: []*types.Value{{Val: &types.Value_Int64Val{Int64Val: 1}}}},
	}

	_, err = fs.GetOnlineFeatures(context.Background(), []string{"fv:f1"}, nil, entities,
		map[string]*types.RepeatedValue{}, true)

	require.NoError(t, err)
	assert.Zero(t, store.plainReads)
	assert.Equal(t, []map[string][]string{{"fv": {"subscriber"}}}, store.entityNamesByView)
}
