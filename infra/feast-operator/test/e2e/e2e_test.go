/*
Copyright 2024 Feast Community.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package e2e

import (
	"github.com/feast-dev/feast/infra/feast-operator/test/utils"
	. "github.com/onsi/ginkgo/v2"
)

var _ = Describe("controller", Ordered, func() {
	featureStoreName := "simple-feast-setup"
	feastResourceName := utils.FeastPrefix + featureStoreName
	feastK8sResourceNames := []string{
		feastResourceName + "-online",
		feastResourceName + "-offline",
		feastResourceName + "-ui",
	}

	runTestDeploySimpleCRFunc := utils.GetTestDeploySimpleCRFunc("/test/e2e",
		"test/testdata/feast_integration_test_crs/v1alpha1_default_featurestore.yaml",
		featureStoreName, feastResourceName, feastK8sResourceNames)

	runTestWithRemoteRegistryFunction := utils.GetTestWithRemoteRegistryFunc("/test/e2e",
		"test/testdata/feast_integration_test_crs/v1alpha1_default_featurestore.yaml",
		"test/testdata/feast_integration_test_crs/v1alpha1_remote_registry_featurestore.yaml",
		featureStoreName, feastResourceName, feastK8sResourceNames)

	BeforeAll(func() {
		utils.DeployOperatorFromCode("/test/e2e", false)
	})

	AfterAll(func() {
		utils.DeleteOperatorDeployment("/test/e2e")
	})

	Context("Operator E2E Tests", func() {
		It("Should be able to deploy and run a default feature store CR successfully", runTestDeploySimpleCRFunc)
		It("Should be able to deploy and run a feature store with remote registry CR successfully", runTestWithRemoteRegistryFunction)
	})
})
