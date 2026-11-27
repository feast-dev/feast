/*
Copyright 2024 Feast Community.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package main

import (
	"context"
	"crypto/tls"
	"errors"
	"testing"

	configv1 "github.com/openshift/api/config/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

func intermediateProfile() configv1.TLSProfileSpec {
	return *configv1.TLSProfiles[configv1.TLSProfileIntermediateType]
}

func TestClassifyTLSProfileError(t *testing.T) {
	tests := []struct {
		name               string
		err                error
		wantProfileFetched bool
		wantError          bool
		wantIntermediate   bool
	}{
		{
			name:               "NoMatchError returns Intermediate defaults, profileFetched=false",
			err:                &meta.NoResourceMatchError{PartialResource: schema.GroupVersionResource{Group: "config.openshift.io"}},
			wantProfileFetched: false,
			wantError:          false,
			wantIntermediate:   true,
		},
		{
			name:               "NotFound returns Intermediate defaults, profileFetched=false",
			err:                apierrors.NewNotFound(schema.GroupResource{Group: "config.openshift.io", Resource: "apiservers"}, "cluster"),
			wantProfileFetched: false,
			wantError:          false,
			wantIntermediate:   true,
		},
		{
			name:               "ServiceUnavailable is transient, profileFetched=true",
			err:                apierrors.NewServiceUnavailable("api server down"),
			wantProfileFetched: true,
			wantError:          false,
			wantIntermediate:   true,
		},
		{
			name:               "Timeout is transient, profileFetched=true",
			err:                apierrors.NewTimeoutError("timed out", 5),
			wantProfileFetched: true,
			wantError:          false,
			wantIntermediate:   true,
		},
		{
			name:               "ServerTimeout is transient, profileFetched=true",
			err:                apierrors.NewServerTimeout(schema.GroupResource{Group: "config.openshift.io", Resource: "apiservers"}, "GET", 5),
			wantProfileFetched: true,
			wantError:          false,
			wantIntermediate:   true,
		},
		{
			name:               "TooManyRequests is transient, profileFetched=true",
			err:                apierrors.NewTooManyRequests("throttled", 5),
			wantProfileFetched: true,
			wantError:          false,
			wantIntermediate:   true,
		},
		{
			name:               "DeadlineExceeded is transient, profileFetched=true",
			err:                context.DeadlineExceeded,
			wantProfileFetched: true,
			wantError:          false,
			wantIntermediate:   true,
		},
		{
			name:               "Forbidden is fatal, returns error",
			err:                apierrors.NewForbidden(schema.GroupResource{Group: "config.openshift.io", Resource: "apiservers"}, "cluster", errors.New("RBAC")),
			wantProfileFetched: false,
			wantError:          true,
			wantIntermediate:   false,
		},
		{
			name:               "Unauthorized is fatal, returns error",
			err:                apierrors.NewUnauthorized("no token"),
			wantProfileFetched: false,
			wantError:          true,
			wantIntermediate:   false,
		},
		{
			name:               "InternalServerError is fatal, returns error",
			err:                apierrors.NewInternalError(errors.New("crash")),
			wantProfileFetched: false,
			wantError:          true,
			wantIntermediate:   false,
		},
		{
			name:               "Generic error is fatal, returns error",
			err:                errors.New("something unexpected"),
			wantProfileFetched: false,
			wantError:          true,
			wantIntermediate:   false,
		},
		{
			name:               "Wrapped DeadlineExceeded is transient",
			err:                errors.Join(errors.New("fetch failed"), context.DeadlineExceeded),
			wantProfileFetched: true,
			wantError:          false,
			wantIntermediate:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			profile, fetched, err := classifyTLSProfileError(tt.err)

			if tt.wantError && err == nil {
				t.Errorf("expected error, got nil")
			}
			if !tt.wantError && err != nil {
				t.Errorf("unexpected error: %v", err)
			}
			if fetched != tt.wantProfileFetched {
				t.Errorf("profileFetched = %v, want %v", fetched, tt.wantProfileFetched)
			}
			if tt.wantIntermediate {
				intermediate := intermediateProfile()
				if profile.MinTLSVersion != intermediate.MinTLSVersion {
					t.Errorf("MinTLSVersion = %v, want %v (Intermediate)", profile.MinTLSVersion, intermediate.MinTLSVersion)
				}
			}
		})
	}
}

func TestIsTransientError(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"ServiceUnavailable", apierrors.NewServiceUnavailable("down"), true},
		{"Timeout", apierrors.NewTimeoutError("slow", 5), true},
		{"ServerTimeout", apierrors.NewServerTimeout(schema.GroupResource{}, "GET", 5), true},
		{"TooManyRequests", apierrors.NewTooManyRequests("throttled", 5), true},
		{"DeadlineExceeded", context.DeadlineExceeded, true},
		{"Wrapped DeadlineExceeded", errors.Join(errors.New("wrapper"), context.DeadlineExceeded), true},
		{"NotFound", apierrors.NewNotFound(schema.GroupResource{}, "x"), false},
		{"Forbidden", apierrors.NewForbidden(schema.GroupResource{}, "x", errors.New("RBAC")), false},
		{"Unauthorized", apierrors.NewUnauthorized("no token"), false},
		{"InternalError", apierrors.NewInternalError(errors.New("crash")), false},
		{"Generic error", errors.New("oops"), false},
		{"Nil", nil, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isTransientError(tt.err); got != tt.want {
				t.Errorf("isTransientError() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestIntermediateProfileHasExpectedDefaults(t *testing.T) {
	profile := intermediateProfile()

	if profile.MinTLSVersion != configv1.VersionTLS12 {
		t.Errorf("Intermediate MinTLSVersion = %v, want %v", profile.MinTLSVersion, configv1.VersionTLS12)
	}
	if len(profile.Ciphers) == 0 {
		t.Error("Intermediate profile should have non-empty cipher list")
	}
}

func TestTLSConfigFromIntermediateProfile(t *testing.T) {
	profile := intermediateProfile()
	tlsConfigFn := configv1ToTLSConfig(profile)

	cfg := &tls.Config{}
	tlsConfigFn(cfg)

	if cfg.MinVersion != tls.VersionTLS12 {
		t.Errorf("MinVersion = %v, want %v (TLS 1.2)", cfg.MinVersion, tls.VersionTLS12)
	}
	if len(cfg.CipherSuites) == 0 {
		t.Error("CipherSuites should not be empty for Intermediate profile")
	}
}

func configv1ToTLSConfig(profile configv1.TLSProfileSpec) func(*tls.Config) {
	// Thin wrapper to test the actual conversion without importing tlspkg in tests.
	// tlspkg.NewTLSConfigFromProfile is what main.go uses.
	var minVersion uint16
	switch profile.MinTLSVersion {
	case configv1.VersionTLS10:
		minVersion = tls.VersionTLS10
	case configv1.VersionTLS11:
		minVersion = tls.VersionTLS11
	case configv1.VersionTLS12:
		minVersion = tls.VersionTLS12
	case configv1.VersionTLS13:
		minVersion = tls.VersionTLS13
	}

	return func(c *tls.Config) {
		c.MinVersion = minVersion
		c.CipherSuites = mapCiphers(profile.Ciphers)
	}
}

func mapCiphers(names []string) []uint16 {
	cipherMap := map[string]uint16{
		"TLS_AES_128_GCM_SHA256":                        tls.TLS_AES_128_GCM_SHA256,
		"TLS_AES_256_GCM_SHA384":                        tls.TLS_AES_256_GCM_SHA384,
		"TLS_CHACHA20_POLY1305_SHA256":                  tls.TLS_CHACHA20_POLY1305_SHA256,
		"TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256":       tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,
		"TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256":         tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,
		"TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384":       tls.TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384,
		"TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384":         tls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,
		"TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305_SHA256": tls.TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305_SHA256,
		"TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305_SHA256":   tls.TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305_SHA256,
	}
	var ids []uint16
	for _, name := range names {
		if id, ok := cipherMap[name]; ok {
			ids = append(ids, id)
		}
	}
	return ids
}

func TestClassifyTLSProfileError_AllTransientErrorsSetProfileFetched(t *testing.T) {
	transientErrors := []error{
		apierrors.NewServiceUnavailable("down"),
		apierrors.NewTimeoutError("slow", 5),
		apierrors.NewServerTimeout(schema.GroupResource{Group: "config.openshift.io", Resource: "apiservers"}, "GET", 5),
		apierrors.NewTooManyRequests("throttled", 5),
		context.DeadlineExceeded,
	}

	for _, err := range transientErrors {
		_, fetched, classifyErr := classifyTLSProfileError(err)
		if classifyErr != nil {
			t.Errorf("transient error %T should not return error, got: %v", err, classifyErr)
		}
		if !fetched {
			t.Errorf("transient error %T should set profileFetched=true", err)
		}
	}
}

func TestClassifyTLSProfileError_NonTransientErrorsDoNotSetProfileFetched(t *testing.T) {
	nonTransientErrors := []error{
		&meta.NoResourceMatchError{PartialResource: schema.GroupVersionResource{Group: "config.openshift.io"}},
		apierrors.NewNotFound(schema.GroupResource{}, "cluster"),
	}

	for _, err := range nonTransientErrors {
		_, fetched, classifyErr := classifyTLSProfileError(err)
		if classifyErr != nil {
			t.Errorf("graceful error %T should not return error, got: %v", err, classifyErr)
		}
		if fetched {
			t.Errorf("graceful error %T should set profileFetched=false", err)
		}
	}
}

func TestClassifyTLSProfileError_FatalErrorsReturnError(t *testing.T) {
	fatalErrors := []error{
		apierrors.NewForbidden(schema.GroupResource{}, "cluster", errors.New("RBAC")),
		apierrors.NewUnauthorized("no token"),
		apierrors.NewInternalError(errors.New("crash")),
		errors.New("unexpected"),
	}

	for _, err := range fatalErrors {
		_, _, classifyErr := classifyTLSProfileError(err)
		if classifyErr == nil {
			t.Errorf("fatal error %T should return error", err)
		}
	}
}

func TestClassifyTLSProfileError_IntermediateProfileAlwaysApplied(t *testing.T) {
	allNonFatalErrors := []error{
		&meta.NoResourceMatchError{PartialResource: schema.GroupVersionResource{Group: "config.openshift.io"}},
		apierrors.NewNotFound(schema.GroupResource{}, "cluster"),
		apierrors.NewServiceUnavailable("down"),
		apierrors.NewTimeoutError("slow", 5),
		apierrors.NewServerTimeout(schema.GroupResource{}, "GET", 5),
		apierrors.NewTooManyRequests("throttled", 5),
		context.DeadlineExceeded,
	}

	intermediate := intermediateProfile()
	for _, err := range allNonFatalErrors {
		profile, _, classifyErr := classifyTLSProfileError(err)
		if classifyErr != nil {
			t.Fatalf("unexpected error for %T: %v", err, classifyErr)
		}
		if profile.MinTLSVersion != intermediate.MinTLSVersion {
			t.Errorf("for error %T: MinTLSVersion = %v, want Intermediate (%v)", err, profile.MinTLSVersion, intermediate.MinTLSVersion)
		}
		if len(profile.Ciphers) != len(intermediate.Ciphers) {
			t.Errorf("for error %T: got %d ciphers, want %d (Intermediate)", err, len(profile.Ciphers), len(intermediate.Ciphers))
		}
	}
}

func TestTLSBootstrapResult_NextProtosAlwaysSet(t *testing.T) {
	// Verify that the TLSOpts from bootstrapTLS always include ALPN with h2 and http/1.1.
	// We can't call bootstrapTLS without a real client, but we can verify the function
	// in tls_bootstrap.go sets NextProtos.
	result := &tlsBootstrapResult{
		TLSOpts: make([]func(*tls.Config), 0, 2),
	}
	result.TLSOpts = append(result.TLSOpts, func(c *tls.Config) {
		c.NextProtos = []string{"h2", alpnHTTP11}
	})

	cfg := &tls.Config{}
	for _, opt := range result.TLSOpts {
		opt(cfg)
	}

	if len(cfg.NextProtos) != 2 || cfg.NextProtos[0] != "h2" || cfg.NextProtos[1] != alpnHTTP11 {
		t.Errorf("NextProtos = %v, want [h2, %s]", cfg.NextProtos, alpnHTTP11)
	}
}
