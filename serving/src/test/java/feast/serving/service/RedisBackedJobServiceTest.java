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
package feast.serving.service;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.codec.ByteArrayCodec;
import java.io.IOException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import redis.embedded.RedisServer;

public class RedisBackedJobServiceTest {

  private static Integer REDIS_PORT = 51235;
  private RedisServer redis;

  @Before
  public void setUp() throws IOException {
    redis = new RedisServer(REDIS_PORT);
    redis.start();
  }

  @After
  public void teardown() {
    redis.stop();
  }

  @Test
  public void shouldRecoverIfRedisConnectionIsLost() {
    RedisClient client = RedisClient.create(RedisURI.create("localhost", REDIS_PORT));
    RedisBackedJobService jobService =
        new RedisBackedJobService(client.connect(new ByteArrayCodec()));
    jobService.get("does not exist");
    redis.stop();
    try {
      jobService.get("does not exist");
    } catch (Exception e) {
      // pass, this should fail, and return a broken connection to the pool
    }
    redis.start();
    jobService.get("does not exist");
    client.shutdown();
  }
}
