package metrics

import (
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/feast-dev/feast/go/internal/feast/registry"
)

const (
	DefaultLookupSampleRate = 1.0
	DefaultFVSampleRate     = 0.01
)

func IsMissingKeyMetricsEnabled() bool {
	return strings.ToLower(os.Getenv("ENABLE_MISSING_KEY_METRICS")) == "true"
}

func IsFVMetricsEnabled() bool {
	return strings.ToLower(os.Getenv("ENABLE_FV_LEVEL_METRICS")) == "true"
}

// IsMetricsClientEnabled returns true if any metrics feature flag is enabled.
func IsMetricsClientEnabled() bool {
	return IsFVMetricsEnabled() || IsMissingKeyMetricsEnabled()
}

func GetOnlineStoreType(config *registry.RepoConfig) string {
	if storeType, ok := config.OnlineStore["type"]; ok {
		return fmt.Sprintf("%v", storeType)
	}
	return "unknown"
}

// ParseLookupSampleRate reads FEAST_METRICS_SAMPLE_RATE for lookup metrics.
func ParseLookupSampleRate() float64 {
	return parseRate(os.Getenv("FEAST_METRICS_SAMPLE_RATE"), DefaultLookupSampleRate)
}

// ParseFVSampleRate reads FEAST_FV_METRICS_SAMPLE_RATE for feature-view read metrics.
func ParseFVSampleRate() float64 {
	return parseRate(os.Getenv("FEAST_FV_METRICS_SAMPLE_RATE"), DefaultFVSampleRate)
}

func parseRate(rateStr string, defaultRate float64) float64 {
	if rateStr == "" {
		return defaultRate
	}
	rate, err := strconv.ParseFloat(rateStr, 64)
	if err != nil || rate <= 0 || rate > 1.0 {
		return defaultRate
	}
	return rate
}

// GetStatsDAddress returns the DogStatsD address from environment variables.
// Returns empty string if DD_AGENT_HOST is not set.
// Port can be configured via DD_DOGSTATSD_PORT (defaults to 8125).
func GetStatsDAddress() string {
	host := os.Getenv("DD_AGENT_HOST")
	if host == "" {
		return ""
	}

	port := os.Getenv("DD_DOGSTATSD_PORT")
	if port == "" {
		port = "8125"
	}

	return fmt.Sprintf("%s:%s", host, port)
}
