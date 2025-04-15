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
	"fmt"
	"os"

	"github.com/feast-dev/feast/infra/feast-operator/test/utils"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("controller", Ordered, func() {
	_, isRunOnOpenShiftCI := os.LookupEnv("RUN_ON_OPENSHIFT_CI")
	featureStoreName := "simple-feast-setup"
	feastResourceName := utils.FeastPrefix + featureStoreName
	feastK8sResourceNames := []string{
		feastResourceName + "-online",
		feastResourceName + "-offline",
		feastResourceName + "-ui",
	}
	namespace := "test-ns-feast"
	defaultFeatureStoreCRTest := "TesDefaultFeastCR"
	remoteRegisteFeatureStoreCRTest := "TestRemoteRegistryFeastCR"

	runTestDeploySimpleCRFunc := utils.GetTestDeploySimpleCRFunc("/test/e2e",
		"test/testdata/feast_integration_test_crs/v1alpha1_default_featurestore.yaml",
		featureStoreName, feastResourceName, feastK8sResourceNames, namespace)

	runTestWithRemoteRegistryFunction := utils.GetTestWithRemoteRegistryFunc("/test/e2e",
		"test/testdata/feast_integration_test_crs/v1alpha1_default_featurestore.yaml",
		"test/testdata/feast_integration_test_crs/v1alpha1_remote_registry_featurestore.yaml",
		featureStoreName, feastResourceName, feastK8sResourceNames, namespace)

	BeforeAll(func() {
		if !isRunOnOpenShiftCI {
			utils.DeployOperatorFromCode("/test/e2e", false)
		}
		By(fmt.Sprintf("Creating test namespace: %s", namespace))
		err := utils.CreateNamespace(namespace, "/test/e2e")
		Expect(err).ToNot(HaveOccurred(), fmt.Sprintf("failed to create namespace %s", namespace))
	})

	AfterAll(func() {
		if !isRunOnOpenShiftCI {
			utils.DeleteOperatorDeployment("/test/e2e")
		}
		By(fmt.Sprintf("Deleting test namespace: %s", namespace))
		err := utils.DeleteNamespace(namespace, "/test/e2e")
		Expect(err).ToNot(HaveOccurred(), fmt.Sprintf("failed to delete namespace %s", namespace))

	})

	Context("Operator E2E Tests", func() {
		It("Should be able to deploy and run a "+defaultFeatureStoreCRTest+" successfully", runTestDeploySimpleCRFunc)
		It("Should be able to deploy and run a "+remoteRegisteFeatureStoreCRTest+"  successfully", runTestWithRemoteRegistryFunction)
	})
})
