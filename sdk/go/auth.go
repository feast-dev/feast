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

// Credential provides OIDC ID tokens used when authenticating with Feast.
// Implements credentials.PerRPCCredentials
type Credential struct {
	tokenSrc oauth2.TokenSource
}

// GetRequestMetadata attaches OIDC token as metadata, refreshing tokens if required.
// This should be called by the GRPC to authenticate each request.
func (provider *Credential) GetRequestMetadata(ctx context.Context, uri ...string) (map[string]string, error) {
	token, err := provider.tokenSrc.Token()
	if err != nil {
		return map[string]string{}, nil
	}
	return map[string]string{
		"Authorization": "Bearer: " + token.AccessToken,
	}, nil
}

// Disable requirement of transport security to allow user to configure it explictly instead.
func (provider *Credential) RequireTransportSecurity() bool {
	return false
}

// Create a Static Authentication Provider that provides a static token
func NewStaticCredential(token string) *Credential {
	return &Credential{tokenSrc: oauth2.StaticTokenSource(
		&oauth2.Token{
			AccessToken: token,
		}),
	}
}

func newGoogleCredential(
	audience string,
	findDefaultCredentials func(ctx context.Context, scopes ...string) (*google.Credentials, error),
	makeTokenSource func(ctx context.Context, audience string, opts ...idtoken.ClientOption) (oauth2.TokenSource, error)) (*Credential, error) {
	// Refresh a Google Id token
	// Attempt to id token from Google Application Default Credentials
	ctx := context.Background()
	creds, err := findDefaultCredentials(ctx, "openid", "email")
	if err != nil {
		return nil, err
	}
	tokenSrc, err := makeTokenSource(ctx, audience, idtoken.WithCredentialsJSON(creds.JSON))
	if err != nil {
		return nil, err
	}
	return &Credential{tokenSrc: tokenSrc}, nil
}

// Creates a new Google Credential which obtains credentials from Application Default Credentials
func NewGoogleCredential(audience string) (*Credential, error) {
	return newGoogleCredential(audience, google.FindDefaultCredentials, idtoken.NewTokenSource)
}

// Creates a new OAuth credential witch obtains credentials by making a client credentials request to an OAuth endpoint.
// clientId, clientSecret - Client credentials used to authenticate the client when obtaining credentials.
// endpointURL - target URL of the OAuth endpoint to make the OAuth request to.
func NewOAuthCredential(audience string, clientId string, clientSecret string, endpointURL *url.URL) *Credential {
	tokenSrc := &oAuthTokenSource{
		clientId:     clientId,
		clientSecret: clientSecret,
		endpointURL:  endpointURL,
		audience:     audience,
	}
	return &Credential{tokenSrc: tokenSrc}
}

// Defines a Token Source that obtains tokens via making a OAuth client credentials request.
type oAuthTokenSource struct {
	clientId     string
	clientSecret string
	endpointURL  *url.URL
	audience     string
	token        *oauth2.Token
}

// Defines a Oauth cleint credentials request.
type oAuthClientCredientialsRequest struct {
	GrantType    string `json:"grant_type"`
	ClientId     string `json:"client_id"`
	ClientSecret string `json:"client_secret"`
	Audience     string `json:"audience"`
}

// Obtain or Refresh token from OAuth Token Source.
func (tokenSrc *oAuthTokenSource) Token() (*oauth2.Token, error) {
	if tokenSrc.token == nil || !tokenSrc.token.Valid() {
		// Refresh Oauth Id token by making Oauth client credentials request
		req := &oAuthClientCredientialsRequest{
			GrantType:    "client_credentials",
			ClientId:     tokenSrc.clientId,
			ClientSecret: tokenSrc.clientSecret,
			Audience:     tokenSrc.audience,
		}

		reqBytes, err := json.Marshal(req)
		if err != nil {
			return nil, err
		}
		resp, err := http.Post(tokenSrc.endpointURL.String(),
			"application/json", bytes.NewBuffer(reqBytes))
		if err != nil {
			return nil, err
		}
		if resp.StatusCode != http.StatusOK {
			return nil, fmt.Errorf("OAuth Endpoint returned unexpected status: %s", resp.Status)
		}
		respBytes, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return nil, err
		}
		tokenSrc.token = &oauth2.Token{}
		err = json.Unmarshal(respBytes, tokenSrc.token)
		if err != nil {
			return nil, err
		}
	}

	return tokenSrc.token, nil
}
