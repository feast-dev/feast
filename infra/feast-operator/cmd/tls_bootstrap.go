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
	"fmt"
	"time"

	configv1 "github.com/openshift/api/config/v1"
	tlspkg "github.com/openshift/controller-runtime-common/pkg/tls"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

const (
	tlsFetchTimeout = 10 * time.Second
	alpnH2          = "h2"
	alpnHTTP11      = "http/1.1"
)

type tlsBootstrapResult struct {
	TLSOpts            []func(*tls.Config)
	ProfileFetched     bool
	ProfileSpec        configv1.TLSProfileSpec
	AdherenceFetched   bool
	AdherencePolicy    configv1.TLSAdherencePolicy
	UnsupportedCiphers []string
}

func fetchTLSProfile(ctx context.Context, k8sClient client.Client) (configv1.TLSProfileSpec, bool, error) {
	fetchCtx, cancel := context.WithTimeout(ctx, tlsFetchTimeout)
	defer cancel()

	profile, err := tlspkg.FetchAPIServerTLSProfile(fetchCtx, k8sClient)
	if err != nil {
		return classifyTLSProfileError(err)
	}
	return profile, true, nil
}

func classifyTLSProfileError(err error) (configv1.TLSProfileSpec, bool, error) {
	intermediate := *configv1.TLSProfiles[configv1.TLSProfileIntermediateType]

	switch {
	case apimeta.IsNoMatchError(err):
		return intermediate, false, nil
	case apierrors.IsNotFound(err):
		return intermediate, false, nil
	case isTransientError(err):
		return intermediate, true, nil
	default:
		return configv1.TLSProfileSpec{}, false, fmt.Errorf("unable to read APIServer TLS profile: %w", err)
	}
}

func fetchTLSAdherencePolicy(ctx context.Context, k8sClient client.Client) (configv1.TLSAdherencePolicy, bool, error) {
	fetchCtx, cancel := context.WithTimeout(ctx, tlsFetchTimeout)
	defer cancel()

	policy, err := tlspkg.FetchAPIServerTLSAdherencePolicy(fetchCtx, k8sClient)
	if err == nil {
		return policy, true, nil
	}

	switch {
	case apimeta.IsNoMatchError(err),
		apierrors.IsNotFound(err),
		isTransientError(err):
		return "", false, nil
	default:
		return "", false, fmt.Errorf("unable to read APIServer TLS adherence policy: %w", err)
	}
}

func bootstrapTLS(ctx context.Context, k8sClient client.Client) (*tlsBootstrapResult, error) {
	logger := log.FromContext(ctx)
	result := &tlsBootstrapResult{
		TLSOpts: make([]func(*tls.Config), 0, 2),
	}

	profile, profileFetched, err := fetchTLSProfile(ctx, k8sClient)
	if err != nil {
		return nil, err
	}
	result.ProfileFetched = profileFetched
	result.ProfileSpec = profile

	tlsConfigFn, unsupported := tlspkg.NewTLSConfigFromProfile(profile)
	result.UnsupportedCiphers = unsupported
	if len(unsupported) > 0 {
		logger.Info("TLS profile contains ciphers unsupported by Go", "unsupported", unsupported)
	}
	result.TLSOpts = append(result.TLSOpts, tlsConfigFn)

	adherence, adherenceFetched, err := fetchTLSAdherencePolicy(ctx, k8sClient)
	if err != nil {
		return nil, err
	}
	result.AdherenceFetched = adherenceFetched
	result.AdherencePolicy = adherence

	result.TLSOpts = append(result.TLSOpts, func(c *tls.Config) {
		c.NextProtos = []string{alpnH2, alpnHTTP11}
	})

	return result, nil
}

func isTransientError(err error) bool {
	return apierrors.IsServiceUnavailable(err) ||
		apierrors.IsTimeout(err) ||
		apierrors.IsServerTimeout(err) ||
		apierrors.IsTooManyRequests(err) ||
		errors.Is(err, context.DeadlineExceeded)
}
