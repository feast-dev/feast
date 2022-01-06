/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright 2018-2019 The Feast Authors
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
package dev.feast;

import com.google.auto.value.AutoValue;
import io.grpc.CallCredentials;
import java.util.Optional;

/** SecurityConfig captures the security related configuration for FeastClient */
@AutoValue
public abstract class SecurityConfig {
  /**
   * Enables authentication If specified, the call credentials used to provide credentials to
   * authenticate with Feast.
   *
   * @return credentials
   */
  public abstract Optional<CallCredentials> getCredentials();

  /**
   * Whether to use TLS transport security is use when connecting to Feast.
   *
   * @return true if enabled
   */
  public abstract boolean isTLSEnabled();

  /**
   * If specified and TLS is enabled, provides path to TLS certificate use the verify Service
   * identity.
   *
   * @return certificate path
   */
  public abstract Optional<String> getCertificatePath();

  @AutoValue.Builder
  public abstract static class Builder {
    public abstract Builder setCredentials(Optional<CallCredentials> credentials);

    public abstract Builder setTLSEnabled(boolean isTLSEnabled);

    public abstract Builder setCertificatePath(Optional<String> certificatePath);

    public abstract SecurityConfig build();
  }

  public static SecurityConfig.Builder newBuilder() {
    return new AutoValue_SecurityConfig.Builder()
        .setCredentials(Optional.empty())
        .setTLSEnabled(false)
        .setCertificatePath(Optional.empty());
  }
}
