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
package feast.serving.configuration;

import io.lettuce.core.api.StatefulRedisConnection;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;

@Configuration
public class StoreConfiguration {

  // We can define other store specific beans here
  // These beans can be autowired or can be created in this class.
  private final StatefulRedisConnection<byte[], byte[]> servingRedisConnection;
  private final StatefulRedisConnection<byte[], byte[]> jobStoreRedisConnection;

  @Autowired
  public StoreConfiguration(
      ObjectProvider<StatefulRedisConnection<byte[], byte[]>> servingRedisConnection,
      ObjectProvider<StatefulRedisConnection<byte[], byte[]>> jobStoreRedisConnection) {
    this.servingRedisConnection = servingRedisConnection.getIfAvailable();
    this.jobStoreRedisConnection = jobStoreRedisConnection.getIfAvailable();
  }

  public StatefulRedisConnection<byte[], byte[]> getServingRedisConnection() {
    return servingRedisConnection;
  }

  public StatefulRedisConnection<byte[], byte[]> getJobStoreRedisConnection() {
    return jobStoreRedisConnection;
  }
}
