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

import com.google.auth.oauth2.IdTokenCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import io.grpc.CallCredentials;
import io.grpc.Metadata;
import io.grpc.Status;
import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.Executor;

/**
 * GoogleAuthCredentials provides a Google OIDC ID token for making authenticated gRPC calls. Uses
 * <a href="https://cloud.google.com/docs/authentication/getting-started">Google Application
 * Default</a> credentials to obtain the OIDC token used for authentication. The given token will be
 * passed as authorization bearer token when making calls.
 */
public class GoogleAuthCredentials extends CallCredentials {
  private final IdTokenCredentials credentials;
  private static final String BEARER_TYPE = "Bearer";
  private static final Metadata.Key<String> AUTHORIZATION_METADATA_KEY =
      Metadata.Key.of("Authorization", ASCII_STRING_MARSHALLER);

  /**
   * Constructa new GoogleAuthCredentials with given options.
   *
   * @param options a map of options, Required unless specified: audience - Optional, Sets the
   *     target audience of the token obtained.
   */
  public GoogleAuthCredentials(Map<String, String> options) throws IOException {
    String targetAudience = options.getOrDefault("audience", "https://localhost");
    ServiceAccountCredentials serviceCreds =
        (ServiceAccountCredentials)
            ServiceAccountCredentials.getApplicationDefault()
                .createScoped(Arrays.asList("openid", "email"));

    credentials =
        IdTokenCredentials.newBuilder()
            .setIdTokenProvider(serviceCreds)
            .setTargetAudience(targetAudience)
            .build();
  }

  @Override
  public void applyRequestMetadata(
      RequestInfo requestInfo, Executor appExecutor, MetadataApplier applier) {
    appExecutor.execute(
        () -> {
          try {
            credentials.refreshIfExpired();
            Metadata headers = new Metadata();
            headers.put(
                AUTHORIZATION_METADATA_KEY,
                String.format("%s %s", BEARER_TYPE, credentials.getIdToken().getTokenValue()));
            applier.apply(headers);
          } catch (Throwable e) {
            applier.fail(Status.UNAUTHENTICATED.withCause(e));
          }
        });
  }

  @Override
  public void thisUsesUnstableApi() {
    // TODO Auto-generated method stub

  }
}
