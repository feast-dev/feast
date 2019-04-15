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
	"encoding/json"
	"time"

	"github.com/golang/protobuf/ptypes/timestamp"

	"github.com/ghodss/yaml"

	"github.com/gojek/feast/protos/generated/go/feast/specs"
	"github.com/gojek/feast/protos/generated/go/feast/types"
)

// YamlToFeatureSpec parses the given yaml and outputs the corresponding
// feature spec, if possible.
func YamlToFeatureSpec(in []byte) (*specs.FeatureSpec, error) {
	var ymlMap map[string]interface{}
	err := yaml.Unmarshal(in, &ymlMap)
	if err != nil {
		return nil, err
	}
	ymlMap["valueType"] = valueTypeOf(ymlMap["valueType"].(string))
	if err != nil {
		return nil, err
	}
	yml, err := yaml.Marshal(ymlMap)
	if err != nil {
		return nil, err
	}
	j, err := yaml.YAMLToJSON(yml)
	if err != nil {
		return nil, err
	}
	var fs specs.FeatureSpec
	err = json.Unmarshal(j, &fs)
	return &fs, err
}

// YamlToEntitySpec parses the given yaml and returns the corresponding entity spec,
// if possible.
func YamlToEntitySpec(in []byte) (*specs.EntitySpec, error) {
	j, err := yaml.YAMLToJSON(in)
	if err != nil {
		return nil, err
	}
	var es specs.EntitySpec
	err = json.Unmarshal(j, &es)
	return &es, err
}

// YamlToFeatureGroupSpec parses the given yaml and returns the corresponding feature
// group spec, if possible.
func YamlToFeatureGroupSpec(in []byte) (*specs.FeatureGroupSpec, error) {
	j, err := yaml.YAMLToJSON(in)
	if err != nil {
		return nil, err
	}
	var fgs specs.FeatureGroupSpec
	err = json.Unmarshal(j, &fgs)
	return &fgs, err
}

// YamlToImportSpec parses the given yaml and returns the corresponding import
// spec, if possible.
func YamlToImportSpec(in []byte) (*specs.ImportSpec, error) {
	var ymlMap map[string]interface{}
	err := yaml.Unmarshal(in, &ymlMap)
	if err != nil {
		return nil, err
	}

	// either timestampValue or timestampColumn
	var timestampValue *timestamp.Timestamp
	var timestampColumn string
	if ymlMap["schema"] != nil {
		schemaYmlMap := ymlMap["schema"].(map[string]interface{})
		if ts, ok := schemaYmlMap["timestampValue"]; ok {
			t, err := time.Parse("2006-01-02T15:04:05.000Z", ts.(string))
			if err != nil {
				return nil, err
			}
			timestampValue = &timestamp.Timestamp{Seconds: t.Unix()}
		}
		if ts, ok := schemaYmlMap["timestampColumn"]; ok {
			timestampColumn = ts.(string)
		}
		if err != nil {
			return nil, err
		}
	}

	j, err := yaml.YAMLToJSON(in)
	if err != nil {
		return nil, err
	}
	var is specs.ImportSpec
	err = json.Unmarshal(j, &is)
	if timestampValue != nil {
		is.Schema.Timestamp = &specs.Schema_TimestampValue{TimestampValue: timestampValue}
	} else if timestampColumn != "" {
		is.Schema.Timestamp = &specs.Schema_TimestampColumn{TimestampColumn: timestampColumn}
	}
	return &is, err
}

func valueTypeOf(str string) types.ValueType_Enum {
	return types.ValueType_Enum(types.ValueType_Enum_value[str])
}
