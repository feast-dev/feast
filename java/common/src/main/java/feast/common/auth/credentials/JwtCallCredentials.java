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

import io.grpc.CallCredentials;
import io.grpc.Metadata;
import java.util.concurrent.Executor;

/**
 * JWTCallCredentials provides/attaches a static JWT token for making authenticated gRPC calls. The
 * given token will be passed as authorization bearer token when making calls.
 */
public final class JwtCallCredentials extends CallCredentials {

  private String jwt;

  public JwtCallCredentials(String jwt) {
    this.jwt = jwt;
  }

  @Override
  public void applyRequestMetadata(
      RequestInfo requestInfo, Executor executor, MetadataApplier metadataApplier) {
    Metadata metadata = new Metadata();
    metadata.put(
        Metadata.Key.of("Authorization", Metadata.ASCII_STRING_MARSHALLER),
        String.format("Bearer %s", jwt));
    metadataApplier.apply(metadata);
  }

  @Override
  public void thisUsesUnstableApi() {
    // does nothing
  }
}
