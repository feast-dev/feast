package feast

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/idtoken"
)

// Returns a mocked google credential.
func mockGoogleCredential(token string, targetAudience string) (*Credential, error) {
	// mock find default credentials implementation.
	findDefaultCredentials := func(ctx context.Context, scopes ...string) (*google.Credentials, error) {
		if len(scopes) != 2 && scopes[0] != "openid" && scopes[1] != "email" {
			return nil, fmt.Errorf("Got bad scopes. Expected 'openid', 'email'")
		}

		return &google.Credentials{
			ProjectID: "project_id",
			JSON:      []byte("mock key json"),
		}, nil
	}

	// mock id token source implementation.
	makeTokenSource := func(ctx context.Context, audience string, opts ...idtoken.ClientOption) (oauth2.TokenSource, error) {
		// unable to check opts as ClientOption refrences internal type.
		if targetAudience != audience {
			return nil, fmt.Errorf("Audience does not match up with target audience")
		}

		return oauth2.StaticTokenSource(&oauth2.Token{
			AccessToken: "google token",
		}), nil
	}

	return newGoogleCredential(targetAudience, findDefaultCredentials, makeTokenSource)
}

// Create a mocked OAuth credential with a backing mocked OAuth server.
func mockOAuthCredential(token string, audience string) (*httptest.Server, *Credential) {
	clientId := "id"
	clientSecret := "secret"
	path := "/oauth"

	// Create a mock OAuth server to test Oauth provider.
	handlers := http.NewServeMux()
	handlers.HandleFunc(path, func(resp http.ResponseWriter, req *http.Request) {
		reqBytes, err := ioutil.ReadAll(req.Body)
		if err != nil {
			resp.WriteHeader(http.StatusBadRequest)
		}

		oauthReq := oAuthClientCredientialsRequest{}
		err = json.Unmarshal(reqBytes, &oauthReq)
		if err != nil {
			resp.WriteHeader(http.StatusBadRequest)
		}

		if oauthReq.GrantType != "client_credentials" ||
			oauthReq.ClientId != clientId ||
			oauthReq.ClientSecret != clientSecret ||
			oauthReq.Audience != audience {
			resp.WriteHeader(http.StatusUnauthorized)
		}

		_, err = resp.Write([]byte(fmt.Sprintf("{\"access_token\": \"%s\"}", token)))
		if err != nil {
			resp.WriteHeader(http.StatusInternalServerError)
		}
	})

	srv := httptest.NewServer(handlers)
	endpointURL, _ := url.Parse(srv.URL + path)
	return srv, NewOAuthCredential(audience, clientId, clientSecret, endpointURL)
}

func TestCredentials(t *testing.T) {
	audience := "localhost"
	srv, oauthCred := mockOAuthCredential("oauth token", audience)
	defer srv.Close()
	googleCred, err := mockGoogleCredential("google token", audience)
	if err != nil {
		t.Errorf("Unexpected error creating mock google credential: %v", err)
	}

	tt := []struct {
		name       string
		credential *Credential
		want       string
		wantErr    bool
		err        error
	}{
		{
			name:       "Valid Static Credential get authentication metadata.",
			credential: NewStaticCredential("static token"),
			want:       "static token",
			wantErr:    false,
			err:        nil,
		},
		{
			name:       "Valid Google Credential get authentication metadata.",
			credential: googleCred,
			want:       "google token",
			wantErr:    false,
			err:        nil,
		},
		{
			name:       "Valid OAuth Credential get authentication metadata.",
			credential: oauthCred,
			want:       "oauth token",
			wantErr:    false,
			err:        nil,
		},
	}

	for _, tc := range tt {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			meta, err := tc.credential.GetRequestMetadata(ctx, "feast.serving")
			if err != nil {
				t.Error(err)
			}
			authKey := "Authorization"
			if _, ok := meta[authKey]; !ok {
				t.Errorf("Expected authentication metadata with key: '%s'", authKey)
			}

			expectedVal := "Bearer: " + tc.want
			if meta[authKey] != expectedVal {
				t.Errorf("Expected authentication metadata with value: '%s' Got instead: '%s'", expectedVal, meta[authKey])
			}
		})
	}
}
