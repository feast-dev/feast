package printer

import (
	"fmt"
	"testing"

	"github.com/gojek/feast/cli/feast/pkg/timeutil"

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
			name: "feature",
			input: &core.UIServiceTypes_FeatureDetail{
				Spec: &specs.FeatureSpec{
					Id:          "test.test_feature_two",
					Owner:       "bob@example.com",
					Name:        "test_feature_two",
					Description: "testing feature",
					Uri:         "https://github.com/bob/example",
					ValueType:   types.ValueType_INT64,
					Entity:      "test",
				},
				BigqueryView: "bqurl",
				Jobs:         []string{"job1", "job2"},
				LastUpdated:  &timestamp.Timestamp{Seconds: 1},
				Created:      &timestamp.Timestamp{Seconds: 1},
			},
			expected: fmt.Sprintf(`Id:	test.test_feature_two
Entity:	test
Owner:	bob@example.com
Description:	testing feature
ValueType:	INT64
Uri:	https://github.com/bob/example
Created:	%s
LastUpdated:	%s
Related Jobs:
- job1
- job2`,
				timeutil.FormatToRFC3339(timestamp.Timestamp{Seconds: 1}),
				timeutil.FormatToRFC3339(timestamp.Timestamp{Seconds: 1})),
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
	expected := fmt.Sprintf(`Name:	test
Description:	my test entity
Tags: tag1,tag2
LastUpdated:	%s
Related Jobs:
- job1
- job2`,
		timeutil.FormatToRFC3339(timestamp.Timestamp{Seconds: 1}))
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
	expected := fmt.Sprintf(`Id:	REDIS1	
Type:	redis	
Options:	
  option1: value1	
  option2: value2	
LastUpdated:	%s`,
		timeutil.FormatToRFC3339(timestamp.Timestamp{Seconds: 1}))
	if out != expected {
		t.Errorf("Expected output:\n%s \nActual:\n%s \n", expected, out)
	}
}
