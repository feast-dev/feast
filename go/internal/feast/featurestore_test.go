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

	"github.com/feast-dev/feast/go/internal/feast/onlinestore"
	"github.com/feast-dev/feast/go/internal/feast/registry"
	"github.com/feast-dev/feast/go/internal/test"
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
