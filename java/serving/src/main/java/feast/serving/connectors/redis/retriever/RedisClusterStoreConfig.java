/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright 2018-2021 The Feast Authors
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
package feast.serving.connectors.redis.retriever;

import io.lettuce.core.ReadFrom;
import java.time.Duration;

public class RedisClusterStoreConfig {
  private final String connectionString;
  private final ReadFrom readFrom;
  private final Duration timeout;
  private final Boolean ssl;
  private final String password;

  public RedisClusterStoreConfig(
      String connectionString, ReadFrom readFrom, Duration timeout, Boolean ssl, String password) {
    this.connectionString = connectionString;
    this.readFrom = readFrom;
    this.timeout = timeout;
    this.ssl = ssl;
    this.password = password;
  }

  public String getConnectionString() {
    return this.connectionString;
  }

  public ReadFrom getReadFrom() {
    return this.readFrom;
  }

  public Duration getTimeout() {
    return this.timeout;
  }

  public Boolean getSsl() {
    return ssl;
  }

  public String getPassword() {
    return password;
  }
}
