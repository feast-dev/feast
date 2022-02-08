package feast

import (
	"fmt"
	"github.com/feast-dev/feast/go/protos/feast/serving"
	"github.com/feast-dev/feast/go/protos/feast/types"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestNewFeatureStore(t *testing.T) {
	config := RepoConfig{
		Project:  "feature_repo_redis",
		Registry: "../test_repo/data/registry.db",
		Provider: "local",
		OnlineStore: map[string]interface{}{
			"type": "redis",
		},
	}
	fs, err := NewFeatureStore(&config)
	assert.Nil(t, err)
	assert.IsType(t, &RedisOnlineStore{}, fs.onlineStore)
}

func TestGetOnlineFeatures1(t *testing.T) {
	config := RepoConfig{
		Project:  "feature_repo_redis",
		Registry: "../test_repo/data/registry.db",
		Provider: "local",
		OnlineStore: map[string]interface{}{
			"type": "redis",
		},
	}

	featureViewNames := []string{"driver_hourly_stats:conv_rate",
		"driver_hourly_stats:acc_rate",
		"driver_hourly_stats:avg_daily_trips"}
	featureList := serving.FeatureList{Val: featureViewNames}
	featureListRequest := serving.GetOnlineFeaturesRequest_Features{Features: &featureList}
	entities := map[string]*types.RepeatedValue{"driver_id": {Val: []*types.Value{{Val: &types.Value_Int64Val{Int64Val: 1001}},
		{Val: &types.Value_Int64Val{Int64Val: 1002}},
		{Val: &types.Value_Int64Val{Int64Val: 1003}}}}}
	request := serving.GetOnlineFeaturesRequest{Kind: &featureListRequest, Entities: entities, FullFeatureNames: true}

	// Kind isGetOnlineFeaturesRequest_Kind `protobuf_oneof:"kind"`
	// // The entity data is specified in a columnar format
	// // A map of entity name -> list of values
	// Entities         map[string]*types.RepeatedValue `protobuf:"bytes,3,rep,name=entities,proto3" json:"entities,omitempty" protobuf_key:"bytes,1,opt,name=key,proto3" protobuf_val:"bytes,2,opt,name=value,proto3"`
	// FullFeatureNames bool                            `protobuf:"varint,4,opt,name=full_feature_names,json=fullFeatureNames,proto3" json:"full_feature_names,omitempty"`
	// // Context for OnDemand Feature Transformation
	// // (was moved to dedicated parameter to avoid unnecessary separation logic on serving side)
	// // A map of variable name -> list of values
	// RequestContext map[string]*types.RepeatedValue `prot

	fs, err := NewFeatureStore(&config)
	assert.Nil(t, err)
	response, err := fs.GetOnlineFeatures(&request)
	assert.Nil(t, err)
	for _, feature_vector := range response.Results {

		values := feature_vector.GetValues()
		statuses := feature_vector.GetStatuses()
		timestamps := feature_vector.GetEventTimestamps()
		lenValues := len(values)
		for i := 0; i < lenValues; i++ {
			fmt.Println(*values[i], statuses[i], timestamps[i].String())
		}
	}
	fmt.Println("Passed featurestore_test")
}
