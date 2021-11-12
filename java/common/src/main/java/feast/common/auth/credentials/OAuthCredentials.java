/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright 2018-2020 The Feast Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package feast.common.auth.credentials;

import static io.grpc.Metadata.ASCII_STRING_MARSHALLER;

import com.nimbusds.jose.util.JSONObjectUtils;
import io.grpc.CallCredentials;
import io.grpc.Metadata;
import io.grpc.Status;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.Executor;
import javax.security.sasl.AuthenticationException;
import net.minidev.json.JSONObject;
import okhttp3.FormBody;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import org.springframework.security.oauth2.jwt.NimbusJwtDecoder;

/**
 * OAuthCredentials uses a OAuth OIDC ID token making authenticated gRPC calls. Makes an OAuth
 * request to obtain the OIDC token used for authentication. The given token will be passed as
 * authorization bearer token when making calls.
 */
public class OAuthCredentials extends CallCredentials {

  private static final String JWK_ENDPOINT_URI = "jwkEndpointURI";
  static final String APPLICATION_JSON = "application/json";
  static final String CONTENT_TYPE = "content-type";
  static final String BEARER_TYPE = "Bearer";
  static final String GRANT_TYPE = "grant_type";
  static final String CLIENT_ID = "client_id";
  static final String CLIENT_SECRET = "client_secret";
  static final String AUDIENCE = "audience";
  static final String OAUTH_URL = "oauth_url";
  static final Metadata.Key<String> AUTHORIZATION_METADATA_KEY =
      Metadata.Key.of("Authorization", ASCII_STRING_MARSHALLER);

  private OkHttpClient httpClient;
  private Request request;
  private String accessToken;
  private Instant tokenExpiryTime;
  private NimbusJwtDecoder jwtDecoder;

  /**
   * Constructs a new OAuthCredentials with given options.
   *
   * @param options a map of options, Required unless specified: grant_type - OAuth grant type.
   *     Should be set as client_credentials audience - Sets the target audience of the token
   *     obtained. client_id - Client id to use in the OAuth request. client_secret - Client securet
   *     to use in the OAuth request. jwtEndpointURI - HTTPS URL used to retrieve a JWK that can be
   *     used to decode the credential.
   */
  public OAuthCredentials(Map<String, String> options) {
    this.httpClient = new OkHttpClient();
    if (!(options.containsKey(GRANT_TYPE)
        && options.containsKey(CLIENT_ID)
        && options.containsKey(AUDIENCE)
        && options.containsKey(CLIENT_SECRET)
        && options.containsKey(OAUTH_URL)
        && options.containsKey(JWK_ENDPOINT_URI))) {
      throw new AssertionError(
          "please configure the properties:"
              + " grant_type, client_id, client_secret, audience, oauth_url, jwkEndpointURI");
    }
    RequestBody requestBody =
        new FormBody.Builder()
            .add(GRANT_TYPE, options.get(GRANT_TYPE))
            .add(CLIENT_ID, options.get(CLIENT_ID))
            .add(CLIENT_SECRET, options.get(CLIENT_SECRET))
            .add(AUDIENCE, options.get(AUDIENCE))
            .build();
    this.request =
        new Request.Builder()
            .url(options.get(OAUTH_URL))
            .addHeader(CONTENT_TYPE, APPLICATION_JSON)
            .post(requestBody)
            .build();
    this.jwtDecoder = NimbusJwtDecoder.withJwkSetUri(options.get(JWK_ENDPOINT_URI)).build();
  }

  @Override
  public void thisUsesUnstableApi() {
    // TODO Auto-generated method stub

  }

  @Override
  public void applyRequestMetadata(
      RequestInfo requestInfo, Executor appExecutor, MetadataApplier applier) {
    appExecutor.execute(
        () -> {
          try {
            // Fetches new token if it is not available or if token has expired.
            if (this.accessToken == null || Instant.now().isAfter(this.tokenExpiryTime)) {
              Response response = httpClient.newCall(request).execute();
              if (!response.isSuccessful()) {
                throw new AuthenticationException(response.message());
              }
              JSONObject json = JSONObjectUtils.parse(response.body().string());
              this.accessToken = json.getAsString("access_token");
              this.tokenExpiryTime = jwtDecoder.decode(this.accessToken).getExpiresAt();
            }
            Metadata headers = new Metadata();
            headers.put(
                AUTHORIZATION_METADATA_KEY, String.format("%s %s", BEARER_TYPE, this.accessToken));
            applier.apply(headers);
          } catch (Throwable e) {
            applier.fail(Status.UNAUTHENTICATED.withCause(e));
          }
        });
  }
}
