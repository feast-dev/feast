package handler

import (
	"context"

	feastdevv1alpha1 "github.com/feast-dev/feast/infra/feast-operator/api/v1alpha1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	FeastPrefix = "feast-"
)

type FeastHandler struct {
	client.Client
	Context      context.Context
	Scheme       *runtime.Scheme
	FeatureStore *feastdevv1alpha1.FeatureStore
}
