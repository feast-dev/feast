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

package previous_version

import (
	"github.com/feast-dev/feast/infra/feast-operator/test/utils"
	. "github.com/onsi/ginkgo/v2"
)

var _ = Describe("operator upgrade", Ordered, func() {
	BeforeAll(func() {
		utils.DeployPreviousVersionOperator()
		utils.DeployOperatorFromCode("/test/e2e", true)
	})

	AfterAll(func() {
		utils.DeleteOperatorDeployment("/test/e2e")
	})

	Context("Operator upgrade Tests", func() {
		runTestDeploySimpleCRFunc := utils.GetTestDeploySimpleCRFunc("/test/upgrade", utils.GetSimplePreviousVerCR(),
			utils.FeatureStoreName, utils.FeastResourceName, []string{})
		runTestWithRemoteRegistryFunction := utils.GetTestWithRemoteRegistryFunc("/test/upgrade", utils.GetSimplePreviousVerCR(),
			utils.GetRemoteRegistryPreviousVerCR(), utils.FeatureStoreName, utils.FeastResourceName, []string{})

		// Run Test on current version operator with previous version CR
		It("Should be able to deploy and run a default feature store CR successfully", runTestDeploySimpleCRFunc)
		It("Should be able to deploy and run a feature store with remote registry CR successfully", runTestWithRemoteRegistryFunction)
	})
})
