package auth

import (
	feastdevv1alpha1 "github.com/feast-dev/feast/infra/feast-operator/api/v1alpha1"
	"github.com/feast-dev/feast/infra/feast-operator/internal/controller/handler"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// FeastAuth is an interface for configuring feast authorization
type FeastAuth struct {
	Handler handler.FeastHandler
}

var (
	feastKubernetesAuthConditions = map[metav1.ConditionStatus]metav1.Condition{
		metav1.ConditionTrue: {
			Type:    feastdevv1alpha1.KubernetesAuthReadyType,
			Status:  metav1.ConditionTrue,
			Reason:  feastdevv1alpha1.ReadyReason,
			Message: feastdevv1alpha1.KubernetesAuthReadyMessage,
		},
		metav1.ConditionFalse: {
			Type:   feastdevv1alpha1.KubernetesAuthReadyType,
			Status: metav1.ConditionFalse,
			Reason: feastdevv1alpha1.KubernetesAuthFailedReason,
		},
	}
)
