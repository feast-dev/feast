package controller

import (
	"context"
	"encoding/json"

	. "github.com/onsi/ginkgo/v2"

	feastdevv1 "github.com/feast-dev/feast/infra/feast-operator/api/v1"
	feastdevv1alpha1 "github.com/feast-dev/feast/infra/feast-operator/api/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	. "github.com/onsi/gomega"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
)

func assertEnvFrom(container corev1.Container) {
	envFrom := container.EnvFrom
	Expect(envFrom).NotTo(BeNil())
	checkEnvFromCounter := 0

	for _, source := range envFrom {
		if source.ConfigMapRef != nil && source.ConfigMapRef.Name == "example-configmap" {
			checkEnvFromCounter += 1
			// Simulate retrieval of ConfigMap data and validate
			configMap := &corev1.ConfigMap{}
			err := k8sClient.Get(context.TODO(), types.NamespacedName{
				Name:      source.ConfigMapRef.Name,
				Namespace: "default",
			}, configMap)
			Expect(err).NotTo(HaveOccurred())
			// Validate a specific key-value pair from the ConfigMap
			Expect(configMap.Data["example-key"]).To(Equal("example-value"))
		}

		if source.SecretRef != nil && source.SecretRef.Name == "example-secret" {
			checkEnvFromCounter += 1
			// Simulate retrieval of Secret data and validate
			secret := &corev1.Secret{}
			err := k8sClient.Get(context.TODO(), types.NamespacedName{
				Name:      source.SecretRef.Name,
				Namespace: "default",
			}, secret)
			Expect(err).NotTo(HaveOccurred())
			// Validate a specific key-value pair from the Secret
			Expect(string(secret.Data["secret-key"])).To(Equal("secret-value"))
		}
	}
	Expect(checkEnvFromCounter).To(Equal(2))
}

func createEnvFromSecretAndConfigMap() {
	By("creating the config map and secret for envFrom")
	envFromConfigMap := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "example-configmap",
			Namespace: "default",
		},
		Data: map[string]string{"example-key": "example-value"},
	}
	err := k8sClient.Create(context.TODO(), envFromConfigMap)
	Expect(err).ToNot(HaveOccurred())

	envFromSecret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "example-secret",
			Namespace: "default",
		},
		StringData: map[string]string{"secret-key": "secret-value"},
	}
	err = k8sClient.Create(context.TODO(), envFromSecret)
	Expect(err).ToNot(HaveOccurred())
}

func deleteEnvFromSecretAndConfigMap() {
	// Delete ConfigMap
	By("Deleting the configmap and secret for envFrom")
	configMap := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "example-configmap",
			Namespace: "default",
		},
	}
	err := k8sClient.Delete(context.TODO(), configMap)
	Expect(err).ToNot(HaveOccurred())

	// Delete Secret
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "example-secret",
			Namespace: "default",
		},
	}
	err = k8sClient.Delete(context.TODO(), secret)
	Expect(err).ToNot(HaveOccurred())
}

func createFeatureStoreResource(resourceName string, image string, pullPolicy corev1.PullPolicy, envVars *[]corev1.EnvVar, envFromVar *[]corev1.EnvFromSource) *feastdevv1.FeatureStore {
	return &feastdevv1.FeatureStore{
		ObjectMeta: metav1.ObjectMeta{
			Name:      resourceName,
			Namespace: "default",
		},
		Spec: feastdevv1.FeatureStoreSpec{
			FeastProject: feastProject,
			Services: &feastdevv1.FeatureStoreServices{
				OfflineStore: &feastdevv1.OfflineStore{
					Server: &feastdevv1.ServerConfigs{
						ContainerConfigs: feastdevv1.ContainerConfigs{
							OptionalCtrConfigs: feastdevv1.OptionalCtrConfigs{
								EnvFrom: envFromVar,
							},
						},
					},
				},
				OnlineStore: &feastdevv1.OnlineStore{
					Server: &feastdevv1.ServerConfigs{
						ContainerConfigs: feastdevv1.ContainerConfigs{
							DefaultCtrConfigs: feastdevv1.DefaultCtrConfigs{
								Image: &image,
							},
							OptionalCtrConfigs: feastdevv1.OptionalCtrConfigs{
								Env:             envVars,
								EnvFrom:         envFromVar,
								ImagePullPolicy: &pullPolicy,
								Resources:       &corev1.ResourceRequirements{},
							},
						},
					},
				},
				Registry: &feastdevv1.Registry{
					Local: &feastdevv1.LocalRegistryConfig{
						Server: &feastdevv1.RegistryServerConfigs{
							ServerConfigs: feastdevv1.ServerConfigs{},
						},
					},
				},
				UI: &feastdevv1.ServerConfigs{
					ContainerConfigs: feastdevv1.ContainerConfigs{
						DefaultCtrConfigs: feastdevv1.DefaultCtrConfigs{
							Image: &image,
						},
						OptionalCtrConfigs: feastdevv1.OptionalCtrConfigs{
							Env:             envVars,
							EnvFrom:         envFromVar,
							ImagePullPolicy: &pullPolicy,
							Resources:       &corev1.ResourceRequirements{},
						},
					},
				},
			},
		},
	}
}

func withEnvFrom() *[]corev1.EnvFromSource {

	return &[]corev1.EnvFromSource{
		{
			ConfigMapRef: &corev1.ConfigMapEnvSource{
				LocalObjectReference: corev1.LocalObjectReference{Name: "example-configmap"},
			},
		},
		{
			SecretRef: &corev1.SecretEnvSource{
				LocalObjectReference: corev1.LocalObjectReference{Name: "example-secret"},
			},
		},
	}

}

// convertV1ToV1Alpha1ForTests converts a v1 FeatureStore to v1alpha1 for use with services package
// This is needed because the services package still uses v1alpha1 internally
func convertV1ToV1Alpha1ForTests(v1Obj *feastdevv1.FeatureStore) *feastdevv1alpha1.FeatureStore {
	v1alpha1Obj := &feastdevv1alpha1.FeatureStore{
		ObjectMeta: v1Obj.ObjectMeta,
	}

	specData, err := json.Marshal(v1Obj.Spec)
	if err != nil {
		return v1alpha1Obj
	}
	if err := json.Unmarshal(specData, &v1alpha1Obj.Spec); err != nil {
		return v1alpha1Obj
	}
	statusData, err := json.Marshal(v1Obj.Status)
	if err != nil {
		return v1alpha1Obj
	}
	if err := json.Unmarshal(statusData, &v1alpha1Obj.Status); err != nil {
		return v1alpha1Obj
	}

	return v1alpha1Obj
}
