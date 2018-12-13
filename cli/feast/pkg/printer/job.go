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
	"fmt"
	"strings"

	"github.com/gojektech/feast/cli/feast/pkg/util"
	"github.com/gojektech/feast/go-feast-proto/feast/core"
)

// PrintJobDetail pretty prints the given job detail
func PrintJobDetail(jobDetail *core.JobServiceTypes_JobDetail) {
	lines := []string{fmt.Sprintf("%s:\t%s", "Id", jobDetail.GetId()),
		fmt.Sprintf("%s:\t%s", "Ext Id", jobDetail.GetExtId()),
		fmt.Sprintf("%s:\t%s", "Type", jobDetail.GetType()),
		fmt.Sprintf("%s:\t%s", "Runner", jobDetail.GetRunner()),
		fmt.Sprintf("%s:\t%s", "Status", jobDetail.GetStatus()),
		fmt.Sprintf("%s:\t%s", "Age", util.ParseAge(*jobDetail.GetCreated())),
	}
	lines = append(lines, "Metrics:")
	for k, v := range jobDetail.GetMetrics() {
		split := strings.Split(k, ":")
		if split[0] == "row" {
			lines = append(lines, fmt.Sprintf("  %s: %.1f", strings.Join(split[1:], ""), v))
		}
	}
	lines = append(lines, "Entities:")
	for _, entity := range jobDetail.GetEntities() {
		lines = append(lines, printEntity(entity, jobDetail.GetMetrics()))
	}
	lines = append(lines, "Features:")
	for _, feature := range jobDetail.GetFeatures() {
		lines = append(lines, printFeature(feature, jobDetail.GetMetrics()))
	}
	fmt.Println(strings.Join(lines, "\n"))
}

func printEntity(entityName string, metrics map[string]float64) string {
	lines := []string{
		fmt.Sprintf("- Name: %s", entityName),
		"  Metrics: ",
	}
	for k, v := range metrics {
		split := strings.Split(k, ":")
		if split[0] == "entity" {
			if split[1] == entityName {
				lines = append(lines, fmt.Sprintf("    %s: %.1f", strings.Join(split[2:], ""), v))
			}
		}
	}
	return strings.Join(lines, "\n")
}

func printFeature(featureName string, metrics map[string]float64) string {
	lines := []string{
		fmt.Sprintf("- Id: %s", featureName),
		"  Metrics: ",
	}
	for k, v := range metrics {
		split := strings.Split(k, ":")
		if split[0] == "feature" {
			if split[1] == featureName {
				lines = append(lines, fmt.Sprintf("    %s: %.1f", strings.Join(split[2:], ""), v))
			}
		}
	}
	return strings.Join(lines, "\n")
}
