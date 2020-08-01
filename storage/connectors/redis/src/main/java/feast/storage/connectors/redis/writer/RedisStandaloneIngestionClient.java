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

import com.google.common.collect.Lists;
import feast.proto.core.StoreProto;
import feast.storage.common.retry.BackOffExecutor;
import io.lettuce.core.LettuceFutures;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.codec.ByteArrayCodec;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.joda.time.Duration;

public class RedisStandaloneIngestionClient implements RedisIngestionClient {
  private final String host;
  private final int port;
  private final BackOffExecutor backOffExecutor;
  private RedisClient redisclient;
  private static final int DEFAULT_TIMEOUT = 2000;
  private StatefulRedisConnection<byte[], byte[]> connection;
  private RedisAsyncCommands<byte[], byte[]> commands;

  public RedisStandaloneIngestionClient(StoreProto.Store.RedisConfig redisConfig) {
    this.host = redisConfig.getHost();
    this.port = redisConfig.getPort();
    long backoffMs = redisConfig.getInitialBackoffMs() > 0 ? redisConfig.getInitialBackoffMs() : 1;
    this.backOffExecutor =
        new BackOffExecutor(redisConfig.getMaxRetries(), Duration.millis(backoffMs));
  }

  @Override
  public void setup() {
    this.redisclient =
        RedisClient.create(new RedisURI(host, port, java.time.Duration.ofMillis(DEFAULT_TIMEOUT)));
  }

  @Override
  public BackOffExecutor getBackOffExecutor() {
    return this.backOffExecutor;
  }

  @Override
  public void shutdown() {
    this.redisclient.shutdown();
  }

  @Override
  public void connect() {
    if (!isConnected()) {
      this.connection = this.redisclient.connect(new ByteArrayCodec());
      this.commands = connection.async();

      // enable pipelining of commands
      this.commands.setAutoFlushCommands(false);
    }
  }

  @Override
  public boolean isConnected() {
    return connection != null;
  }

  @Override
  public void sync(Iterable<Future<?>> futures) {
    this.connection.flushCommands();

    LettuceFutures.awaitAll(
        60, TimeUnit.SECONDS, Lists.newArrayList(futures).toArray(new Future[0]));
  }

  @Override
  public CompletableFuture<String> set(byte[] key, byte[] value) {
    return commands.set(key, value).toCompletableFuture();
  }

  @Override
  public CompletableFuture<byte[]> get(byte[] key) {
    return commands.get(key).toCompletableFuture();
  }
}
