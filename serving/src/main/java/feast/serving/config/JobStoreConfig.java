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
package feast.serving.config;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.resource.DefaultClientResources;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;

@Configuration
public class JobStoreConfig {

  private final StatefulRedisConnection<byte[], byte[]> jobStoreRedisConnection;

  @Autowired
  public JobStoreConfig(FeastProperties feastProperties) {
    RedisURI uri =
        RedisURI.create(
            feastProperties.getJobStore().getRedisHost(),
            feastProperties.getJobStore().getRedisPort());

    jobStoreRedisConnection =
        RedisClient.create(DefaultClientResources.create(), uri).connect(new ByteArrayCodec());
  }

  public StatefulRedisConnection<byte[], byte[]> getJobStoreRedisConnection() {
    return jobStoreRedisConnection;
  }
}
