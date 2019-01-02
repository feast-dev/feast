// Copyright 2018 The Feast Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package parse

import (
	"testing"

	"github.com/golang/protobuf/ptypes/timestamp"

	"github.com/gojek/feast/protos/generated/go/feast/specs"
	"github.com/gojek/feast/protos/generated/go/feast/types"

	"github.com/google/go-cmp/cmp"
)

func TestYamlToFeatureSpec(t *testing.T) {
	tt := []struct {
		name     string
		input    []byte
		expected *specs.FeatureSpec
		err      error
	}{
		{
			name: "valid yaml",
			input: []byte(`id: test.none.test_feature_two
name: test_feature_two
entity: test
owner: bob@example.com
description: testing feature
valueType:  INT64
granularity: NONE
uri: https://github.com/bob/example
dataStores:
  serving:
    id: REDIS
  warehouse:
    id: BIGQUERY`),
			expected: &specs.FeatureSpec{
				Id:          "test.none.test_feature_two",
				Owner:       "bob@example.com",
				Name:        "test_feature_two",
				Description: "testing feature",
				Uri:         "https://github.com/bob/example",
				Granularity: types.Granularity_NONE,
				ValueType:   types.ValueType_INT64,
				Entity:      "test",
				DataStores: &specs.DataStores{
					Serving: &specs.DataStore{
						Id: "REDIS",
					},
					Warehouse: &specs.DataStore{
						Id: "BIGQUERY",
					},
				},
			},
			err: nil,
		},
	}

	for _, tc := range tt {
		t.Run(tc.name, func(t *testing.T) {
			spec, err := YamlToFeatureSpec(tc.input)
			if tc.err == nil {
				if err != nil {
					t.Error(err)
				} else if !cmp.Equal(spec, tc.expected) {
					t.Errorf("Expected %s, got %s", tc.expected, spec)
				}
			} else {
				// we expect an error
				if err == nil {
					t.Error(err)
				} else if err.Error() != tc.err.Error() {
					t.Errorf("Expected error %v, got %v", err.Error(), tc.err.Error())
				}
			}
		})
	}
}

func TestYamlToEntitySpec(t *testing.T) {
	tt := []struct {
		name     string
		input    []byte
		expected *specs.EntitySpec
		err      error
	}{
		{
			name: "valid yaml",
			input: []byte(`name: test
description: test entity
tags:
- tag1
- tag2`),
			expected: &specs.EntitySpec{
				Name:        "test",
				Description: "test entity",
				Tags:        []string{"tag1", "tag2"},
			},
			err: nil,
		},
	}

	for _, tc := range tt {
		t.Run(tc.name, func(t *testing.T) {
			spec, err := YamlToEntitySpec(tc.input)
			if tc.err == nil {
				if err != nil {
					t.Error(err)
				} else if !cmp.Equal(spec, tc.expected) {
					t.Errorf("Expected %s, got %s", tc.expected, spec)
				}
			} else {
				// we expect an error
				if err == nil {
					t.Error(err)
				} else if err.Error() != tc.err.Error() {
					t.Errorf("Expected error %v, got %v", err.Error(), tc.err.Error())
				}
			}
		})
	}
}

func TestYamlToFeatureGroupSpec(t *testing.T) {
	tt := []struct {
		name     string
		input    []byte
		expected *specs.FeatureGroupSpec
		err      error
	}{
		{
			name: "valid yaml",
			input: []byte(`id: test
tags:
- tag1
- tag2
dataStores:
  serving:
    id: REDIS
  warehouse:
    id: BIGQUERY`),
			expected: &specs.FeatureGroupSpec{
				Id:   "test",
				Tags: []string{"tag1", "tag2"},
				DataStores: &specs.DataStores{
					Serving: &specs.DataStore{
						Id: "REDIS",
					},
					Warehouse: &specs.DataStore{
						Id: "BIGQUERY",
					},
				},
			},
			err: nil,
		},
	}

	for _, tc := range tt {
		t.Run(tc.name, func(t *testing.T) {
			spec, err := YamlToFeatureGroupSpec(tc.input)
			if tc.err == nil {
				if err != nil {
					t.Error(err)
				} else if !cmp.Equal(spec, tc.expected) {
					t.Errorf("Expected %s, got %s", tc.expected, spec)
				}
			} else {
				// we expect an error
				if err == nil {
					t.Error(err)
				} else if err.Error() != tc.err.Error() {
					t.Errorf("Expected error %v, got %v", err.Error(), tc.err.Error())
				}
			}
		})
	}
}

func TestYamlToStorageSpec(t *testing.T) {
	tt := []struct {
		name     string
		input    []byte
		expected *specs.StorageSpec
		err      error
	}{
		{
			name: "valid yaml",
			input: []byte(`id: REDIS1
type: REDIS
options:
  redis.host: localhost`),
			expected: &specs.StorageSpec{
				Id:   "REDIS1",
				Type: "REDIS",
				Options: map[string]string{
					"redis.host": "localhost",
				},
			},
			err: nil,
		},
	}

	for _, tc := range tt {
		t.Run(tc.name, func(t *testing.T) {
			spec, err := YamlToStorageSpec(tc.input)
			if tc.err == nil {
				if err != nil {
					t.Error(err)
				} else if !cmp.Equal(spec, tc.expected) {
					t.Errorf("Expected %s, got %s", tc.expected, spec)
				}
			} else {
				// we expect an error
				if err == nil {
					t.Error(err)
				} else if err.Error() != tc.err.Error() {
					t.Errorf("Expected error %v, got %v", err.Error(), tc.err.Error())
				}
			}
		})
	}
}
func TestYamlToImportSpec(t *testing.T) {
	tt := []struct {
		name     string
		input    []byte
		expected *specs.ImportSpec
		err      error
	}{
		{
			name: "valid yaml",
			input: []byte(`type: file
options:
  format: csv
  path: jaeger_last_opportunity_sample.csv
entities:
  - driver
schema:
  entityIdColumn: driver_id
  timestampValue: 2018-09-25T00:00:00.000Z
  fields:
    - name: timestamp
    - name: driver_id
    - name: last_opportunity
      featureId: driver.none.last_opportunity`),
			expected: &specs.ImportSpec{
				Type: "file",
				Options: map[string]string{
					"format": "csv",
					"path":   "jaeger_last_opportunity_sample.csv",
				},
				Entities: []string{"driver"},
				Schema: &specs.Schema{
					Fields: []*specs.Field{
						{Name: "timestamp"},
						{Name: "driver_id"},
						{Name: "last_opportunity", FeatureId: "driver.none.last_opportunity"},
					},
					EntityIdColumn: "driver_id",
					Timestamp: &specs.Schema_TimestampValue{
						TimestampValue: &timestamp.Timestamp{Seconds: 1537833600},
					},
				},
			},
			err: nil,
		},
	}

	for _, tc := range tt {
		t.Run(tc.name, func(t *testing.T) {
			spec, err := YamlToImportSpec(tc.input)
			if tc.err == nil {
				if err != nil {
					t.Error(err)
				} else if !cmp.Equal(spec, tc.expected) {
					t.Errorf("Expected %s, got %s", tc.expected, spec)
				}
			} else {
				// we expect an error
				if err == nil {
					t.Error(err)
				} else if err.Error() != tc.err.Error() {
					t.Errorf("Expected error %v, got %v", err.Error(), tc.err.Error())
				}
			}
		})
	}
}

func TestYamlToImportSpecNoSchema(t *testing.T) {
	tt := []struct {
		name     string
		input    []byte
		expected *specs.ImportSpec
		err      error
	}{
		{
			name: "valid yaml",
			input: []byte(`type: pubsub
options:
  topic: projects/your-gcp-project/topics/feast-test
entities:
  - testentity`),
			expected: &specs.ImportSpec{
				Type: "pubsub",
				Options: map[string]string{
					"topic": "projects/your-gcp-project/topics/feast-test",
				},
				Entities: []string{"testentity"},
			},
			err: nil,
		},
	}

	for _, tc := range tt {
		t.Run(tc.name, func(t *testing.T) {
			spec, err := YamlToImportSpec(tc.input)
			if tc.err == nil {
				if err != nil {
					t.Error(err)
				} else if !cmp.Equal(spec, tc.expected) {
					t.Errorf("Expected %s, got %s", tc.expected, spec)
				}
			} else {
				// we expect an error
				if err == nil {
					t.Error(err)
				} else if err.Error() != tc.err.Error() {
					t.Errorf("Expected error %v, got %v", err.Error(), tc.err.Error())
				}
			}
		})
	}
}
