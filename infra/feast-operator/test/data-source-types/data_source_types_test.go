package datasources

import (
	"bufio"
	"os"
	"slices"
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	feastdevv1alpha1 "github.com/feast-dev/feast/infra/feast-operator/api/v1alpha1"
	"github.com/feast-dev/feast/infra/feast-operator/internal/controller/services"
)

func TestDataSourceTypes(t *testing.T) {
	RegisterFailHandler(Fail)

	RunSpecs(t, "Data Source Suite")
}

var _ = Describe("FeatureStore Data Source Types", func() {
	Context("When checking against the python code in feast.repo_config", func() {
		It("should match defined registry persistence types in the operator", func() {
			registryFilePersistenceTypes := []string{string(services.RegistryFileConfigType)}
			registryPersistenceTypes := append(feastdevv1alpha1.ValidRegistryDBStorePersistenceTypes, registryFilePersistenceTypes...)
			checkPythonPersistenceTypes("registry.out", registryPersistenceTypes)
		})
		It("should match defined onlineStore persistence types in the operator", func() {
			onlineFilePersistenceTypes := []string{string(services.OnlineSqliteConfigType)}
			onlinePersistenceTypes := append(feastdevv1alpha1.ValidOnlineStoreDBStorePersistenceTypes, onlineFilePersistenceTypes...)
			checkPythonPersistenceTypes("online-store.out", onlinePersistenceTypes)
		})
		It("should match defined offlineStore persistence types in the operator", func() {
			offlinePersistenceTypes := append(feastdevv1alpha1.ValidOfflineStoreDBStorePersistenceTypes, feastdevv1alpha1.ValidOfflineStoreFilePersistenceTypes...)
			checkPythonPersistenceTypes("offline-store.out", offlinePersistenceTypes)
		})
	})
})

func checkPythonPersistenceTypes(fileName string, operatorDsTypes []string) {
	feastDsTypes, err := readFileLines(fileName)
	Expect(err).NotTo(HaveOccurred())

	// Add remote type to slice, as its not a file or db type and we want to limit its use to registry service when deploying with the operator
	operatorDsTypes = append(operatorDsTypes, "remote")
	missingFeastTypes := []string{}
	for _, ods := range operatorDsTypes {
		if len(ods) > 0 {
			if !slices.Contains(feastDsTypes, ods) {
				missingFeastTypes = append(missingFeastTypes, ods)
			}
		}
	}
	Expect(missingFeastTypes).To(BeEmpty())

	missingOperatorTypes := []string{}
	for _, fds := range feastDsTypes {
		if len(fds) > 0 {
			if !slices.Contains(operatorDsTypes, fds) {
				missingOperatorTypes = append(missingOperatorTypes, fds)
			}
		}
	}
	Expect(missingOperatorTypes).To(BeEmpty())
}

func readFileLines(filePath string) ([]string, error) {
	file, err := os.Open(filePath)
	Expect(err).NotTo(HaveOccurred())
	defer closeFile(file)

	var lines []string
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		lines = append(lines, scanner.Text())
	}

	err = scanner.Err()
	Expect(err).NotTo(HaveOccurred())

	return lines, nil
}

func closeFile(file *os.File) {
	err := file.Close()
	Expect(err).NotTo(HaveOccurred())
}
