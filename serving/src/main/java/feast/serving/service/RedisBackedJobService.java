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
package feast.serving.service;

import com.google.protobuf.util.JsonFormat;
import feast.serving.ServingAPIProto.Job;
import feast.serving.ServingAPIProto.Job.Builder;
import feast.serving.config.FeastProperties;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.resource.DefaultClientResources;
import java.util.Optional;
import org.joda.time.Duration;
import org.slf4j.Logger;

// TODO: Do rate limiting, currently if clients call get() or upsert()
//       and an exceedingly high rate e.g. they wrap job reload in a while loop with almost no wait
//       Redis connection may break and need to restart Feast serving. Need to handle this.

public class RedisBackedJobService implements JobService {

  private static final Logger log = org.slf4j.LoggerFactory.getLogger(RedisBackedJobService.class);
  private final RedisCommands<byte[], byte[]> syncCommand;
  // Remove job state info after "defaultExpirySeconds" to prevent filling up Redis memory
  // and since users normally don't require info about relatively old jobs.
  private final int defaultExpirySeconds = (int) Duration.standardDays(1).getStandardSeconds();

  public RedisBackedJobService(FeastProperties.JobStoreProperties jobStoreProperties) {
    RedisURI uri =
        RedisURI.create(jobStoreProperties.getRedisHost(), jobStoreProperties.getRedisPort());

    this.syncCommand =
        RedisClient.create(DefaultClientResources.create(), uri)
            .connect(new ByteArrayCodec())
            .sync();
  }

  public RedisBackedJobService(StatefulRedisConnection<byte[], byte[]> connection) {
    this.syncCommand = connection.sync();
  }

  @Override
  public Optional<Job> get(String id) {
    Job job = null;
    try {
      String json = new String(syncCommand.get(id.getBytes()));
      if (json.isEmpty()) {
        return Optional.empty();
      }
      Builder builder = Job.newBuilder();
      JsonFormat.parser().merge(json, builder);
      job = builder.build();
    } catch (Exception e) {
      log.error(String.format("Failed to parse JSON for Feast job: %s", e.getMessage()));
    }
    return Optional.ofNullable(job);
  }

  @Override
  public void upsert(Job job) {
    try {
      syncCommand.set(
          job.getId().getBytes(),
          JsonFormat.printer().omittingInsignificantWhitespace().print(job).getBytes());
      syncCommand.expire(job.getId().getBytes(), defaultExpirySeconds);
    } catch (Exception e) {
      log.error(String.format("Failed to upsert job: %s", e.getMessage()));
    }
  }
}
