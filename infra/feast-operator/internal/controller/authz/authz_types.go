package authz

import (
	feastdevv1alpha1 "github.com/feast-dev/feast/infra/feast-operator/api/v1alpha1"
	"github.com/feast-dev/feast/infra/feast-operator/internal/controller/handler"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// FeastAuthorization is an interface for configuring feast authorization
type FeastAuthorization struct {
	Handler handler.FeastHandler
}

var (
	feastKubernetesAuthConditions = map[metav1.ConditionStatus]metav1.Condition{
		metav1.ConditionTrue: {
			Type:    feastdevv1alpha1.AuthorizationReadyType,
			Status:  metav1.ConditionTrue,
			Reason:  feastdevv1alpha1.ReadyReason,
			Message: feastdevv1alpha1.KubernetesAuthzReadyMessage,
		},
		metav1.ConditionFalse: {
			Type:   feastdevv1alpha1.AuthorizationReadyType,
			Status: metav1.ConditionFalse,
			Reason: feastdevv1alpha1.KubernetesAuthzFailedReason,
		},
	}
)
