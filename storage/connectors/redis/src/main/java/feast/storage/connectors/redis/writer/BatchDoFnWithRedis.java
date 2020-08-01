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
package feast.storage.connectors.redis.writer;

import feast.storage.common.retry.Retriable;
import io.lettuce.core.RedisException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.function.Function;
import org.apache.beam.sdk.transforms.DoFn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base class for redis-related DoFns. Assumes that operations will be batched. Prepares redisClient
 * on DoFn.Setup stage and close it on DoFn.Teardown stage.
 *
 * @param <Input>
 * @param <Output>
 */
public class BatchDoFnWithRedis<Input, Output> extends DoFn<Input, Output> {
  private static final Logger log = LoggerFactory.getLogger(BatchDoFnWithRedis.class);

  private final RedisIngestionClient redisIngestionClient;

  BatchDoFnWithRedis(RedisIngestionClient redisIngestionClient) {
    this.redisIngestionClient = redisIngestionClient;
  }

  @Setup
  public void setup() {
    this.redisIngestionClient.setup();
  }

  @StartBundle
  public void startBundle() {
    try {
      redisIngestionClient.connect();
    } catch (RedisException e) {
      log.error("Connection to redis cannot be established: %s", e);
    }
  }

  void executeBatch(Function<RedisIngestionClient, Iterable<Future<? extends Object>>> executor)
      throws Exception {
    this.redisIngestionClient
        .getBackOffExecutor()
        .execute(
            new Retriable() {
              @Override
              public void execute() throws ExecutionException, InterruptedException {
                if (!redisIngestionClient.isConnected()) {
                  redisIngestionClient.connect();
                }

                Iterable<Future<?>> futures = executor.apply(redisIngestionClient);
                redisIngestionClient.sync(futures);
              }

              @Override
              public Boolean isExceptionRetriable(Exception e) {
                return e instanceof RedisException;
              }

              @Override
              public void cleanUpAfterFailure() {}
            });
  }

  @Teardown
  public void teardown() {
    redisIngestionClient.shutdown();
  }
}
