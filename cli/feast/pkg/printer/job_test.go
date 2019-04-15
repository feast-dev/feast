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

package printer

import (
	"testing"
)

func Test_printEntity(t *testing.T) {
	type args struct {
		entityName string
		metrics    map[string]float64
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{"1 feature 1 metric value", args{"entity1", map[string]float64{"entity:entity1:value1": 89.0}}, "- Name: entity1\n  Metrics: \n    value1: 89.0"},
		{"1 feature 2 metric value", args{"entity1", map[string]float64{"entity:entity1:value1": 89.0, "entity:entity1:value2": 90.0}}, "- Name: entity1\n  Metrics: \n    value1: 89.0\n    value2: 90.0"},
		{"1 feature 0 metric value", args{"entity1", map[string]float64{"entity:entity2:value1": 89.0, "entity:entity3:value2": 90.0}}, "- Name: entity1\n  Metrics: "},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := printEntity(tt.args.entityName, tt.args.metrics); got != tt.want {
				t.Errorf("printEntity() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_printFeature(t *testing.T) {
	type args struct {
		featureName string
		metrics     map[string]float64
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{"1 feature 1 metric value", args{"feature1", map[string]float64{"feature:feature1:value1": 89.0, "feature:feature2:value1": 90.0}}, "- Id: feature1\n  Metrics: \n    value1: 89.0"},
		{"1 feature 2 metric value", args{"feature1", map[string]float64{"feature:feature1:value1": 89.0, "feature:feature1:value2": 90.0}}, "- Id: feature1\n  Metrics: \n    value1: 89.0\n    value2: 90.0"},
		{"1 feature 0 metric value", args{"feature1", map[string]float64{"feature:feature2:value1": 89.0, "feature:feature3:value1": 90.0}}, "- Id: feature1\n  Metrics: "},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := printFeature(tt.args.featureName, tt.args.metrics); got != tt.want {
				t.Errorf("printEntity() = %v, want %v", got, tt.want)
				println(len(got), len(tt.want))
			}
		})
	}
}
