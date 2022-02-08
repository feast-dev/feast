//go:build integration
// +build integration

package feast

import (
	"fmt"
	"github.com/feast-dev/feast/go/protos/feast/serving"
	"github.com/feast-dev/feast/go/protos/feast/types"
	"github.com/stretchr/testify/assert"
	"io/ioutil"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

func setupFeastRepo(tb testing.TB, featureStoreYaml string) (func(testing.TB), string) {
	// Use time as unique identifier for test
	t := time.Now()
	project := "test_" + t.Format("20060102150405")

	// create temp dir for the feature store repo and data
	dir, err := ioutil.TempDir("", project)
	if err != nil {
		log.Fatal(err)
	}

	// TODO: Use testcontainers to start Redis

	// Init feast repo
	// TODO (woop): Use Feast from repo
	cmd := exec.Command("sh", "-c", "feast init repo")
	cmd.Dir = dir
	out, err := cmd.CombinedOutput()
	fmt.Printf("%s\n", out)
	if err != nil {
		tb.Errorf("%v", err)
	}
	dir = filepath.Join(dir, "/repo")

	// Configure feature_store.yaml
	ioutil.WriteFile(dir+"/feature_store.yaml", []byte(featureStoreYaml), 0644)

	// run apply
	cmd = exec.Command("sh", "-c", "feast apply")
	cmd.Dir = dir
	out, err = cmd.CombinedOutput()
	fmt.Printf("%s\n", out)
	if err != nil {
		tb.Errorf("%v", err)
	}

	// run materialize
	cmd = exec.Command("sh", "-c", "feast materialize-incremental $(date -u +\"%Y-%m-%dT%H:%M:%S\")")
	cmd.Dir = dir
	out, err = cmd.CombinedOutput()
	fmt.Printf("%s\n", out)
	if err != nil {
		tb.Errorf("%v", err)
	}

	return func(tb testing.TB) {
		os.RemoveAll(dir)
	}, dir
}

func TestFeatureStore(t *testing.T) {

	defaultFeatureStoreYaml := `project: test_repo
registry: data/registry.db
provider: local
online_store:
    type: redis
    connection_string: "localhost:6379"`

	tests := []struct {
		name             string
		featureStoreYaml string
		useDefaultRepo   bool
		repoContents     string
		request          serving.GetOnlineFeaturesRequest
		expected         serving.GetOnlineFeaturesResponse
		wantError        bool
		want             string
	}{
		{
			"multiple_features_single_entity_multiple_entity_values",
			defaultFeatureStoreYaml,
			true,
			"",
			serving.GetOnlineFeaturesRequest{Kind: &serving.GetOnlineFeaturesRequest_Features{
				Features: &serving.FeatureList{Val: []string{
					"driver_hourly_stats:conv_rate",
					"driver_hourly_stats:acc_rate",
					"driver_hourly_stats:avg_daily_trips"}}},
				Entities: map[string]*types.RepeatedValue{"driver_id": {Val: []*types.Value{
					{Val: &types.Value_Int64Val{Int64Val: 1001}},
					{Val: &types.Value_Int64Val{Int64Val: 1002}},
					{Val: &types.Value_Int64Val{Int64Val: 1003}}}}},
				FullFeatureNames: true},
			serving.GetOnlineFeaturesResponse{
				Metadata: nil,
				Results:  nil,
			},
			false,
			"",
		},
		{
			"multiple_features_multiple_entities_multiple_entity_values",
			defaultFeatureStoreYaml,
			true,
			"",
			serving.GetOnlineFeaturesRequest{Kind: &serving.GetOnlineFeaturesRequest_Features{
				Features: &serving.FeatureList{Val: []string{
					"driver_hourly_stats:conv_rate",
					"driver_hourly_stats:acc_rate",
					"driver_hourly_stats:avg_daily_trips"}}},
				Entities: map[string]*types.RepeatedValue{"driver_id": {Val: []*types.Value{
					{Val: &types.Value_Int64Val{Int64Val: 1001}},
					{Val: &types.Value_Int64Val{Int64Val: 1002}},
					{Val: &types.Value_Int64Val{Int64Val: 1003}}}},
					"customer_id": {Val: []*types.Value{
						{Val: &types.Value_Int64Val{Int64Val: 2001}},
						{Val: &types.Value_Int64Val{Int64Val: 2002}},
						{Val: &types.Value_Int64Val{Int64Val: 2003}}}}},
				FullFeatureNames: true},
			serving.GetOnlineFeaturesResponse{
				Metadata: nil,
				Results:  nil,
			},
			false,
			"",
		},
		{
			"no_feature_references",
			defaultFeatureStoreYaml,
			true,
			"",
			serving.GetOnlineFeaturesRequest{Kind: &serving.GetOnlineFeaturesRequest_Features{
				Features: &serving.FeatureList{Val: []string{}}},
				Entities: map[string]*types.RepeatedValue{"driver_id": {Val: []*types.Value{
					{Val: &types.Value_Int64Val{Int64Val: 1001}},
					{Val: &types.Value_Int64Val{Int64Val: 1002}},
					{Val: &types.Value_Int64Val{Int64Val: 1003}}}}},
				FullFeatureNames: true},
			serving.GetOnlineFeaturesResponse{
				Metadata: nil,
				Results:  nil,
			},
			true,
			"No feature references provided",
		},
		{
			"no_entity_keys",
			defaultFeatureStoreYaml,
			true,
			"",
			serving.GetOnlineFeaturesRequest{Kind: &serving.GetOnlineFeaturesRequest_Features{
				Features: &serving.FeatureList{Val: []string{}}},
				Entities: map[string]*types.RepeatedValue{"driver_id": {Val: []*types.Value{
					{Val: &types.Value_Int64Val{Int64Val: 1001}},
					{Val: &types.Value_Int64Val{Int64Val: 1002}},
					{Val: &types.Value_Int64Val{Int64Val: 1003}}}}},
				FullFeatureNames: true},
			serving.GetOnlineFeaturesResponse{
				Metadata: nil,
				Results:  nil,
			},
			true,
			"No entity keys provided",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			teardown, dir := setupFeastRepo(t, tc.featureStoreYaml)
			defer teardown(t)

			conf, err := NewRepoConfig(dir)
			if err != nil {
				t.Errorf("%v", err)
			}

			fs, err := NewFeatureStore(conf)
			if err != nil {
				t.Errorf("%v", err)
			}

			actual, err := fs.GetOnlineFeatures(&tc.request)

			if err != nil && !tc.wantError {
				t.Errorf("%v", err)
			}

			//if tc.wantError {
			//	if err == nil {
			//		t.Errorf("Expected error with message %s, but no error returned", tc.want)
			//	}
			//
			//	if err.Error() != tc.want {
			//		t.Errorf("hello() = %v, want %v", err, tc.want)
			//	}
			//
			//}

			if !assert.Equal(t, actual, tc.expected) {
				//t.Errorf("expected %v, got %v", tc.expected, actual)
				// TODO: Enable these tests when GetOnlineFeaturesWorks
				println("These tests are being skipped for now!!")
			}
		})
	}
}
