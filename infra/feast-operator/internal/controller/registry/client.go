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

package registry

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"
)

const (
	permissionsPath = "/api/v1/permissions"
	requestTimeout  = 10 * time.Second
)

// PermissionPolicy holds the extracted policy data from a Feast permission.
type PermissionPolicy struct {
	Groups     []string
	Namespaces []string
}

// ListPermissions fetches permissions from the registry REST API for the given project.
// Uses cluster-internal TLS (InsecureSkipVerify for service certs).
// Returns policies from GroupBasedPolicy, NamespaceBasedPolicy, and CombinedGroupNamespacePolicy only.
func ListPermissions(ctx context.Context, registryRestURL string, project string) ([]PermissionPolicy, error) {
	if registryRestURL == "" || project == "" {
		return nil, nil
	}
	baseURL := registryRestURL
	if !strings.HasPrefix(baseURL, "http://") && !strings.HasPrefix(baseURL, "https://") {
		baseURL = "https://" + baseURL
	}
	u, err := url.Parse(baseURL)
	if err != nil {
		return nil, fmt.Errorf("failed to parse registry URL: %w", err)
	}
	u.Path = strings.TrimSuffix(u.Path, "/") + permissionsPath
	q := u.Query()
	q.Set("project", project)
	u.RawQuery = q.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}
	client := &http.Client{
		Timeout: requestTimeout,
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		},
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch permissions: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("registry returned status %d", resp.StatusCode)
	}
	var result struct {
		Permissions []struct {
			Spec *struct {
				Policy map[string]json.RawMessage `json:"policy,omitempty"`
			} `json:"spec,omitempty"`
		} `json:"permissions,omitempty"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("failed to decode permissions response: %w", err)
	}
	var policies []PermissionPolicy
	for _, p := range result.Permissions {
		if p.Spec == nil || p.Spec.Policy == nil {
			continue
		}
		pol := extractPolicy(p.Spec.Policy)
		if pol != nil {
			policies = append(policies, *pol)
		}
	}
	return policies, nil
}

func extractPolicy(policy map[string]json.RawMessage) *PermissionPolicy {
	var groups, namespaces []string
	for k, v := range policy {
		norm := strings.ToLower(strings.ReplaceAll(k, "_", ""))
		switch norm {
		case "groupbasedpolicy":
			var g struct {
				Groups []string `json:"groups,omitempty"`
			}
			if err := json.Unmarshal(v, &g); err == nil {
				groups = append(groups, g.Groups...)
			}
		case "namespacebasedpolicy":
			var n struct {
				Namespaces []string `json:"namespaces,omitempty"`
			}
			if err := json.Unmarshal(v, &n); err == nil {
				namespaces = append(namespaces, n.Namespaces...)
			}
		case "combinedgroupnamespacepolicy":
			var c struct {
				Groups     []string `json:"groups,omitempty"`
				Namespaces []string `json:"namespaces,omitempty"`
			}
			if err := json.Unmarshal(v, &c); err == nil {
				groups = append(groups, c.Groups...)
				namespaces = append(namespaces, c.Namespaces...)
			}
		}
	}
	if len(groups) == 0 && len(namespaces) == 0 {
		return nil
	}
	return &PermissionPolicy{Groups: groups, Namespaces: namespaces}
}
