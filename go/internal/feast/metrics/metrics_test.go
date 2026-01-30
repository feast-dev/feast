package metrics

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMetricsInit(t *testing.T) {
	InitMetrics()

	// We can't easily check the global registry without possibly conflicting with other tests or init order,
	// but we can check if the struct fields are populated (not nil)

	assert.NotNil(t, Metrics.HttpDuration)
	assert.NotNil(t, Metrics.GrpcDuration)
	assert.NotNil(t, Metrics.HttpRequestsTotal)

	// Call them to ensure no panic
	Metrics.HttpRequestsTotal(HttpLabels{Method: "GET", Status: 200, Path: "/"}).Inc()
}
