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
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestExtractPolicy_GroupBasedPolicy(t *testing.T) {
	policy := map[string]json.RawMessage{
		"groupBasedPolicy": json.RawMessage(`{"groups":["admin-group","data-team"]}`),
	}
	result := extractPolicy(policy)
	if result == nil {
		t.Fatal("expected non-nil result")
	}
	assertStrSlice(t, result.Groups, []string{"admin-group", "data-team"})
	assertStrSlice(t, result.Namespaces, nil)
}

func TestExtractPolicy_NamespaceBasedPolicy(t *testing.T) {
	policy := map[string]json.RawMessage{
		"namespaceBasedPolicy": json.RawMessage(`{"namespaces":["ds-project-1","ds-project-2"]}`),
	}
	result := extractPolicy(policy)
	if result == nil {
		t.Fatal("expected non-nil result")
	}
	assertStrSlice(t, result.Groups, nil)
	assertStrSlice(t, result.Namespaces, []string{"ds-project-1", "ds-project-2"})
}

func TestExtractPolicy_CombinedGroupNamespacePolicy(t *testing.T) {
	policy := map[string]json.RawMessage{
		"combinedGroupNamespacePolicy": json.RawMessage(`{"groups":["ml-engineers"],"namespaces":["prod-ns"]}`),
	}
	result := extractPolicy(policy)
	if result == nil {
		t.Fatal("expected non-nil result")
	}
	assertStrSlice(t, result.Groups, []string{"ml-engineers"})
	assertStrSlice(t, result.Namespaces, []string{"prod-ns"})
}

func TestExtractPolicy_RoleBasedPolicyIgnored(t *testing.T) {
	policy := map[string]json.RawMessage{
		"roleBasedPolicy": json.RawMessage(`{"roles":["admin"]}`),
	}
	result := extractPolicy(policy)
	if result != nil {
		t.Fatalf("expected nil for RoleBasedPolicy, got %+v", result)
	}
}

func TestExtractPolicy_EmptyPolicy(t *testing.T) {
	result := extractPolicy(map[string]json.RawMessage{})
	if result != nil {
		t.Fatalf("expected nil for empty policy, got %+v", result)
	}
}

func TestExtractPolicy_SnakeCaseKeys(t *testing.T) {
	// Verify normalization handles snake_case (e.g. from alternative serializers)
	policy := map[string]json.RawMessage{
		"group_based_policy": json.RawMessage(`{"groups":["team-a"]}`),
	}
	result := extractPolicy(policy)
	if result == nil {
		t.Fatal("expected non-nil result for snake_case key")
	}
	assertStrSlice(t, result.Groups, []string{"team-a"})
}

func TestListPermissions_EmptyInputs(t *testing.T) {
	policies, err := ListPermissions(context.Background(), "", "project", "tok", false)
	if err != nil || policies != nil {
		t.Fatalf("expected nil,nil for empty URL; got %v, %v", policies, err)
	}
	policies, err = ListPermissions(context.Background(), "http://localhost", "", "tok", false)
	if err != nil || policies != nil {
		t.Fatalf("expected nil,nil for empty project; got %v, %v", policies, err)
	}
}

func TestListPermissions_Success(t *testing.T) {
	responseBody := `{
		"permissions": [
			{
				"spec": {
					"name": "perm1",
					"project": "my_project",
					"policy": {
						"groupBasedPolicy": {"groups": ["group-a", "group-b"]}
					}
				}
			},
			{
				"spec": {
					"name": "perm2",
					"project": "my_project",
					"policy": {
						"namespaceBasedPolicy": {"namespaces": ["ns-1"]}
					}
				}
			},
			{
				"spec": {
					"name": "perm3",
					"project": "my_project",
					"policy": {
						"roleBasedPolicy": {"roles": ["admin"]}
					}
				}
			}
		]
	}`
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != permissionsPath {
			t.Errorf("unexpected path: %s", r.URL.Path)
		}
		if r.URL.Query().Get("project") != "my_project" {
			t.Errorf("unexpected project param: %s", r.URL.Query().Get("project"))
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(responseBody))
	}))
	defer server.Close()

	policies, err := ListPermissions(context.Background(), server.URL, "my_project", "test-token", false)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// RoleBasedPolicy should be filtered out, leaving 2 policies
	if len(policies) != 2 {
		t.Fatalf("expected 2 policies, got %d", len(policies))
	}
	assertStrSlice(t, policies[0].Groups, []string{"group-a", "group-b"})
	assertStrSlice(t, policies[1].Namespaces, []string{"ns-1"})
}

func TestListPermissions_NonOKStatus(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	_, err := ListPermissions(context.Background(), server.URL, "p", "tok", false)
	if err == nil {
		t.Fatal("expected error for non-200 status")
	}
}

func TestListPermissions_EmptyPermissions(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"permissions":[]}`))
	}))
	defer server.Close()

	policies, err := ListPermissions(context.Background(), server.URL, "p", "tok", false)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(policies) != 0 {
		t.Fatalf("expected 0 policies, got %d", len(policies))
	}
}

func TestListPermissions_NoSpecPermission(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"permissions":[{}]}`))
	}))
	defer server.Close()

	policies, err := ListPermissions(context.Background(), server.URL, "p", "tok", false)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(policies) != 0 {
		t.Fatalf("expected 0 policies for nil spec, got %d", len(policies))
	}
}

func TestListPermissions_URLWithoutScheme_TLS(t *testing.T) {
	_, err := ListPermissions(context.Background(), "127.0.0.1:8080", "p", "tok", true)
	if err == nil {
		t.Fatal("expected error for unreachable host")
	}
}

func TestListPermissions_URLWithoutScheme_NoTLS(t *testing.T) {
	_, err := ListPermissions(context.Background(), "127.0.0.1:8080", "p", "tok", false)
	if err == nil {
		t.Fatal("expected error for unreachable host")
	}
}

func TestBuildIntraCommunicationJWT(t *testing.T) {
	secretToken := "my-random-secret-value" // pragma: allowlist secret
	jwt := BuildIntraCommunicationJWT(secretToken)
	parts := strings.Split(jwt, ".")
	if len(parts) != 3 {
		t.Fatalf("expected 3 JWT parts, got %d", len(parts))
	}

	payloadBytes, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		t.Fatalf("failed to decode payload: %v", err)
	}
	var payload struct {
		Sub string `json:"sub"`
	}
	if err := json.Unmarshal(payloadBytes, &payload); err != nil {
		t.Fatalf("failed to unmarshal payload: %v", err)
	}
	expectedSub := ":::" + secretToken
	if payload.Sub != expectedSub {
		t.Fatalf("expected sub %q, got %q", expectedSub, payload.Sub)
	}
}

func TestListPermissions_SendsIntraCommunicationJWT(t *testing.T) {
	var receivedAuth string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedAuth = r.Header.Get("Authorization")
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"permissions":[]}`))
	}))
	defer server.Close()

	_, err := ListPermissions(context.Background(), server.URL, "p", "my-secret", false)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.HasPrefix(receivedAuth, "Bearer ") {
		t.Fatalf("expected Bearer token, got %q", receivedAuth)
	}
	token := strings.TrimPrefix(receivedAuth, "Bearer ")
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		t.Fatalf("expected valid JWT with 3 parts, got %d", len(parts))
	}
}

func TestListPermissions_NoTokenNoAuthHeader(t *testing.T) {
	var receivedAuth string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedAuth = r.Header.Get("Authorization")
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"permissions":[]}`))
	}))
	defer server.Close()

	_, err := ListPermissions(context.Background(), server.URL, "p", "", false)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if receivedAuth != "" {
		t.Fatalf("expected no Authorization header when token is empty, got %q", receivedAuth)
	}
}

func assertStrSlice(t *testing.T, got, want []string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("slice length mismatch: got %v, want %v", got, want)
	}
	for i := range got {
		if got[i] != want[i] {
			t.Fatalf("slice[%d] mismatch: got %q, want %q", i, got[i], want[i])
		}
	}
}
