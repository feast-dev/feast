package printer

import (
	"testing"

	"github.com/golang/protobuf/ptypes/timestamp"

	"github.com/gojek/feast/protos/generated/go/feast/core"
	"github.com/gojek/feast/protos/generated/go/feast/specs"
	"github.com/gojek/feast/protos/generated/go/feast/types"
)

func TestPrintFeature(t *testing.T) {
	tt := []struct {
		name     string
		input    *core.UIServiceTypes_FeatureDetail
		expected string
	}{
		{
			name: "with storage",
			input: &core.UIServiceTypes_FeatureDetail{
				Spec: &specs.FeatureSpec{
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
				BigqueryView: "bqurl",
				Jobs:         []string{"job1", "job2"},
				LastUpdated:  &timestamp.Timestamp{Seconds: 1},
				Created:      &timestamp.Timestamp{Seconds: 1},
			},
			expected: `Id:	test.none.test_feature_two
Entity:	test
Owner:	bob@example.com
Description:	testing feature
ValueType:	INT64
Uri:	https://github.com/bob/example
DataStores: 
  Serving:	REDIS
  Warehouse:	BIGQUERY
Created:	1970-01-01T07:30:01+07:30
LastUpdated:	1970-01-01T07:30:01+07:30
Related Jobs:
- job1
- job2`,
		}, {
			name: "no storage",
			input: &core.UIServiceTypes_FeatureDetail{
				Spec: &specs.FeatureSpec{
					Id:          "test.none.test_feature_two",
					Owner:       "bob@example.com",
					Name:        "test_feature_two",
					Description: "testing feature",
					Uri:         "https://github.com/bob/example",
					Granularity: types.Granularity_NONE,
					ValueType:   types.ValueType_INT64,
					Entity:      "test",
				},
				BigqueryView: "bqurl",
				Jobs:         []string{"job1", "job2"},
				LastUpdated:  &timestamp.Timestamp{Seconds: 1},
				Created:      &timestamp.Timestamp{Seconds: 1},
			},
			expected: `Id:	test.none.test_feature_two
Entity:	test
Owner:	bob@example.com
Description:	testing feature
ValueType:	INT64
Uri:	https://github.com/bob/example
Created:	1970-01-01T07:30:01+07:30
LastUpdated:	1970-01-01T07:30:01+07:30
Related Jobs:
- job1
- job2`,
		},
	}

	for _, tc := range tt {
		t.Run(tc.name, func(t *testing.T) {
			out := PrintFeatureDetail(tc.input)
			if out != tc.expected {
				t.Errorf("Expected output:\n%s \nActual:\n%s \n", tc.expected, out)
			}
		})
	}
}

func TestPrintEntity(t *testing.T) {
	entityDetail := &core.UIServiceTypes_EntityDetail{
		Spec: &specs.EntitySpec{
			Name:        "test",
			Description: "my test entity",
			Tags:        []string{"tag1", "tag2"},
		},
		Jobs:        []string{"job1", "job2"},
		LastUpdated: &timestamp.Timestamp{Seconds: 1},
	}
	out := PrintEntityDetail(entityDetail)
	expected := `Name:	test
Description:	my test entity
Tags: tag1,tag2
LastUpdated:	1970-01-01T07:30:01+07:30
Related Jobs:
- job1
- job2`
	if out != expected {
		t.Errorf("Expected output:\n%s \nActual:\n%s \n", expected, out)
	}
}

func TestPrintStorage(t *testing.T) {
	storageDetail := &core.UIServiceTypes_StorageDetail{
		Spec: &specs.StorageSpec{
			Id:   "REDIS1",
			Type: "redis",
			Options: map[string]string{
				"option1": "value1",
				"option2": "value2",
			},
		},
		LastUpdated: &timestamp.Timestamp{Seconds: 1},
	}
	out := PrintStorageDetail(storageDetail)
	expected := `Id:	REDIS1
Type:	redis
Options:
  option1: value1
  option2: value2
LastUpdated:	1970-01-01T07:30:01+07:30`
	if out != expected {
		t.Errorf("Expected output:\n%s \nActual:\n%s \n", expected, out)
	}
}
