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
import java.util.Optional;
import org.joda.time.Duration;
import org.slf4j.Logger;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.exceptions.JedisException;

// TODO: Do rate limiting, currently if clients call get() or upsert()
//       and an exceedingly high rate e.g. they wrap job reload in a while loop with almost no wait
//       Redis connection may break and need to restart Feast serving. Need to handle this.

public class RedisBackedJobService implements JobService {

  private static final Logger log = org.slf4j.LoggerFactory.getLogger(RedisBackedJobService.class);
  private final JedisPool jedisPool;
  // Remove job state info after "defaultExpirySeconds" to prevent filling up Redis memory
  // and since users normally don't require info about relatively old jobs.
  private final int defaultExpirySeconds = (int) Duration.standardDays(1).getStandardSeconds();

  public RedisBackedJobService(JedisPool jedisPool) {
    this.jedisPool = jedisPool;
  }

  @Override
  public Optional<Job> get(String id) {
    Jedis jedis = null;
    Job job = null;
    try {
      jedis = jedisPool.getResource();
      String json = jedis.get(id);
      if (json == null) {
        return Optional.empty();
      }
      Builder builder = Job.newBuilder();
      JsonFormat.parser().merge(json, builder);
      job = builder.build();
    } catch (JedisException e) {
      log.error(String.format("Failed to connect to the redis instance: %s", e));
    } catch (Exception e) {
      log.error(String.format("Failed to parse JSON for Feast job: %s", e.getMessage()));
    } finally {
      if (jedis != null) {
        jedis.close();
      }
    }
    return Optional.ofNullable(job);
  }

  @Override
  public void upsert(Job job) {
    Jedis jedis = null;
    try {
      jedis = jedisPool.getResource();
      jedis.set(job.getId(), JsonFormat.printer().omittingInsignificantWhitespace().print(job));
      jedis.expire(job.getId(), defaultExpirySeconds);
    } catch (Exception e) {
      log.error(String.format("Failed to upsert job: %s", e.getMessage()));
    } finally {
      if (jedis != null) {
        jedis.close();
      }
    }
  }
}
