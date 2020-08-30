package feast

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/idtoken"
	"io/ioutil"
	"net/http"
	"net/url"
)

// AuthProvider defines an Authentication Provider that provides OIDC ID tokens
// to be use when authenticating with Feast.
type AuthProvider interface {
	// Get a OIDC ID token that can be used for authenticating with Feast.
	// audience - Target audience of the obtained credentials.
	Token(audience string) (string, error)
}

// Defines a AuthProvider that uses a static token
type StaticProvider struct {
	// Static token that the AuthProvider uses authenticate.
	StaticToken string
}

func (provider *StaticProvider) Token(_ string) (string, error) {
	return provider.StaticToken, nil
}

// Google Authentication Provider obtains credentials from Application Default Credentials
type GoogleProvider struct {
	findDefaultCredentials func(ctx context.Context, scopes ...string) (*google.Credentials, error)
	makeTokenSource        func(ctx context.Context, audience string, opts ...idtoken.ClientOption) (oauth2.TokenSource, error)
	token                  *oauth2.Token
	audience               string
}

func NewGoogleProvider() *GoogleProvider {
	return &GoogleProvider{
		findDefaultCredentials: google.FindDefaultCredentials,
		makeTokenSource:        idtoken.NewTokenSource,
	}
}

/// Backend Google OAuth used by GoogleProvider to source credentials.
func (provider *GoogleProvider) Token(audience string) (string, error) {
	if provider.token == nil || !provider.token.Valid() || provider.audience != audience {
		// Refresh a Google Id token
		// Attempt to id token from Google Application Default Credentials
		ctx := context.Background()
		creds, err := provider.findDefaultCredentials(ctx, "openid", "email")
		if err != nil {
			return "", err
		}
		src, err := provider.makeTokenSource(ctx, audience, idtoken.WithCredentialsJSON(creds.JSON))
		if err != nil {
			return "", err
		}
		provider.token, err = src.Token()
		provider.audience = audience
		if err != nil {
			return "", err
		}
	}
	return provider.token.AccessToken, nil
}

// OAuth Provider obtains credentials by making a client credentials request to
// an OAuth endpoint.
type OAuthProvider struct {
	// Client credentials used to authenticate the client when obtaining credentials.
	ClientId     string
	ClientSecret string
	EndpointURL  *url.URL
	token        *oauth2.Token
	audience     string
}

func (provider *OAuthProvider) Token(audience string) (string, error) {
	if provider.token == nil || !provider.token.Valid() || provider.audience != audience {
		// Refresh Oauth Id token by making Oauth client credentials request
		reqMap := map[string]string{
			"grant_type":    "client_credentials",
			"client_id":     provider.ClientId,
			"client_secret": provider.ClientSecret,
			"audience":      audience,
		}
		reqBytes, err := json.Marshal(reqMap)
		if err != nil {
			return "", err
		}
		resp, err := http.Post(provider.EndpointURL.String(),
			"application/json", bytes.NewBuffer(reqBytes))
		if err != nil {
			return "", err
		}
		if resp.StatusCode != http.StatusOK {
			return "", fmt.Errorf("OAuth Endpoint returned unexpected status: %s", resp.Status)
		}
		respBytes, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return "", err
		}
		provider.token = &oauth2.Token{}
		provider.audience = audience
		err = json.Unmarshal(respBytes, provider.token)
		if err != nil {
			return "", err
		}
	}

	return provider.token.AccessToken, nil
}
