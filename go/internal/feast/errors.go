package feast

import (
	"fmt"
	"strings"
)

// TODO: Add more errors here especially once that have been created in python sdk
// and coordinate error exceptions in GoServer in python sdk
type FeatureNameCollisionError struct {
	featureRefCollisions []string
	fullFeatureNames     bool
}

func NewFeatureNameCollisionError(featureRefCollisions []string, fullFeatureNames bool) FeatureNameCollisionError {
	return FeatureNameCollisionError{featureRefCollisions, fullFeatureNames}
}

func (e FeatureNameCollisionError) Error() string {
	return fmt.Sprintf("featureNameCollisionError: %s; %t", strings.Join(e.featureRefCollisions, ", "), e.fullFeatureNames)
}
