package feast

import (
	"fmt"
	"strings"
)

type FeatureNameCollisionError struct {
	featureRefCollisions []string
	fullFeatureNames     bool
}

func NewFeatureNameCollisionError(featureRefCollisions []string, fullFeatureNames bool) *FeatureNameCollisionError {
	return &FeatureNameCollisionError{featureRefCollisions, fullFeatureNames}
}

func (e *FeatureNameCollisionError) Error() string {
	return fmt.Sprintf("featureNameCollisionError: %s; %t", strings.Join(e.featureRefCollisions, ", "), e.fullFeatureNames)
}
