package handler

import (
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// delete an object if the FeatureStore is set as the object's controller/owner
func (handler *FeastHandler) DeleteOwnedFeastObj(obj client.Object) error {
	name := obj.GetName()
	kind := obj.GetObjectKind().GroupVersionKind().Kind
	if err := handler.Client.Get(handler.Context, client.ObjectKeyFromObject(obj), obj); err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return err
	}
	for _, ref := range obj.GetOwnerReferences() {
		if *ref.Controller && ref.UID == handler.FeatureStore.UID {
			if err := handler.Client.Delete(handler.Context, obj); err != nil {
				return err
			}
			log.FromContext(handler.Context).Info("Successfully deleted", kind, name)
		}
	}
	return nil
}
