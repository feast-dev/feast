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

public class RedisStoreConfig {
  private final String host;
  private final Integer port;
  private final Boolean ssl;
  private final String password;

  public RedisStoreConfig(String host, Integer port, Boolean ssl, String password) {
    this.host = host;
    this.port = port;
    this.ssl = ssl;
    this.password = password;
  }

  public String getHost() {
    return this.host;
  }

  public Integer getPort() {
    return this.port;
  }

  public Boolean getSsl() {
    return this.ssl;
  }

  public String getPassword() {
    return this.password;
  }
}
