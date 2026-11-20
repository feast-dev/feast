package onlinestore

// Parsing and validation of the Bigtable online store's online_store config.

import (
	"errors"
	"fmt"
	"math"
	"time"
)

// bigtableConfig is the parsed online_store section of feature_store.yaml. MaxInflightReads is zero, no
// limit, only when max_inflight_reads is unset (the key itself must be positive); a zero MaxInflightWait
// rejects a read at once when max_inflight_reads is full.
type bigtableConfig struct {
	ProjectID, Instance, AppProfile                       string
	KeysPerRequest, MaxConcurrentChunks, MaxInflightReads int
	ReadTimeout, MaxInflightWait                          time.Duration
	AdaptiveChunking, DisableMetrics                      bool
}

// parseBigtableConfig reads the online_store config map and applies defaults; validate checks the ranges.
// Numbers must be whole and positive (YAML gives float64), switches must be YAML booleans, a null keeps the
// default as in Python, and unknown keys are ignored.
func parseBigtableConfig(m map[string]interface{}) (bigtableConfig, error) {
	cfg := bigtableConfig{
		KeysPerRequest:      bigtableDefaultKeysPerRequest,
		MaxConcurrentChunks: bigtableDefaultMaxConcurrentChunks,
	}
	if err := parseBigtableStrings(m, &cfg); err != nil {
		return bigtableConfig{}, err
	}
	if err := parseBigtableInts(m, &cfg); err != nil {
		return bigtableConfig{}, err
	}
	for _, f := range []struct {
		key string
		dst *bool
	}{
		{"adaptive_chunking", &cfg.AdaptiveChunking},
		{"disable_metrics", &cfg.DisableMetrics},
	} {
		if err := optionalBool(m, f.key, f.dst); err != nil {
			return bigtableConfig{}, err
		}
	}
	return cfg, nil
}

// validate fails unless keys_per_request is 1 to bigtableMaxKeysPerRequest (this store's cap per ReadRows,
// which adaptive_chunking grows to), max_concurrent_chunks and read_timeout_ms are positive, and
// max_inflight_reads and max_inflight_wait_ms are not negative.
func (cfg bigtableConfig) validate() error {
	if cfg.KeysPerRequest < 1 || cfg.KeysPerRequest > bigtableMaxKeysPerRequest {
		return fmt.Errorf("bigtable online store: keys_per_request must be between 1 and %d, got %d",
			bigtableMaxKeysPerRequest, cfg.KeysPerRequest)
	}
	if cfg.MaxConcurrentChunks < 1 {
		return fmt.Errorf("bigtable online store: max_concurrent_chunks must be positive, got %d", cfg.MaxConcurrentChunks)
	}
	if cfg.ReadTimeout <= 0 {
		return fmt.Errorf("bigtable online store: read_timeout_ms must be positive, got %d", cfg.ReadTimeout.Milliseconds())
	}
	if cfg.MaxInflightReads < 0 {
		return fmt.Errorf("bigtable online store: max_inflight_reads must not be negative, got %d", cfg.MaxInflightReads)
	}
	if cfg.MaxInflightWait < 0 {
		return fmt.Errorf("bigtable online store: max_inflight_wait_ms must not be negative, got %d",
			cfg.MaxInflightWait.Milliseconds())
	}
	return nil
}

func parseBigtableStrings(m map[string]interface{}, cfg *bigtableConfig) error {
	for _, f := range []struct {
		key string
		dst *string
	}{
		{"instance", &cfg.Instance},
		{"project_id", &cfg.ProjectID},
		{"app_profile", &cfg.AppProfile},
	} {
		if err := optionalString(m, f.key, f.dst); err != nil {
			return err
		}
	}
	if cfg.Instance == "" {
		return errors.New("bigtable online store: instance is required")
	}
	return nil
}

// parseBigtableInts reads the numeric keys. max_versions is Python's GC rule; this reader takes the latest
// cell whatever it is, so it is not read.
func parseBigtableInts(m map[string]interface{}, cfg *bigtableConfig) error {
	readTimeoutMs := int(bigtableDefaultReadTimeout.Milliseconds())
	for _, f := range []struct {
		key string
		dst *int
	}{
		{"keys_per_request", &cfg.KeysPerRequest},
		{"max_concurrent_chunks", &cfg.MaxConcurrentChunks},
		{"read_timeout_ms", &readTimeoutMs},
		{"max_inflight_reads", &cfg.MaxInflightReads},
	} {
		if err := optionalPositiveInt(m, f.key, f.dst); err != nil {
			return err
		}
	}
	cfg.ReadTimeout = time.Duration(readTimeoutMs) * time.Millisecond
	inflightWaitMs, err := optionalNonNegativeInt(m, "max_inflight_wait_ms")
	if err != nil {
		return err
	}
	cfg.MaxInflightWait = time.Duration(inflightWaitMs) * time.Millisecond
	return nil
}

// optionalString sets *dst to a present, non-null string m[key]; a null (`key:`, or an unset ${VAR}) keeps it.
func optionalString(m map[string]interface{}, key string, dst *string) error {
	raw := m[key]
	if raw == nil {
		return nil
	}
	s, ok := raw.(string)
	if !ok {
		return fmt.Errorf("bigtable online store: %s must be a string, got %T", key, raw)
	}
	*dst = s
	return nil
}

// optionalBool sets *dst to a present, non-null bool m[key].
func optionalBool(m map[string]interface{}, key string, dst *bool) error {
	raw := m[key]
	if raw == nil {
		return nil
	}
	b, ok := raw.(bool)
	if !ok {
		return fmt.Errorf("bigtable online store: %s must be true or false, got %T %v", key, raw, raw)
	}
	*dst = b
	return nil
}

// optionalPositiveInt sets *dst to a present, non-null m[key] that is a whole number from 1 to MaxInt32.
func optionalPositiveInt(m map[string]interface{}, key string, dst *int) error {
	return optionalWholeNumber(m, key, dst, 1, "a positive integer")
}

// optionalNonNegativeInt returns m[key] when it is a whole number from 0 to MaxInt32, and 0 when it is
// absent or null.
func optionalNonNegativeInt(m map[string]interface{}, key string) (int, error) {
	var n int
	err := optionalWholeNumber(m, key, &n, 0, "a non-negative integer")
	return n, err
}

// optionalWholeNumber sets *dst to a present, non-null m[key] that is a whole number from lowest to
// MaxInt32; want names the accepted range in the error.
func optionalWholeNumber(m map[string]interface{}, key string, dst *int, lowest float64, want string) error {
	raw := m[key]
	if raw == nil {
		return nil
	}
	var f float64
	switch v := raw.(type) {
	case float64:
		f = v
	case int:
		f = float64(v)
	default:
		return fmt.Errorf("bigtable online store: %s must be %s, got %T %v", key, want, raw, raw)
	}
	if f != math.Trunc(f) || f < lowest || f > math.MaxInt32 {
		return fmt.Errorf("bigtable online store: %s must be %s, got %v", key, want, raw)
	}
	*dst = int(f)
	return nil
}
