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
package feast.storage.connectors.redis.write;

import com.google.common.collect.Lists;
import feast.core.StoreProto;
import feast.storage.common.retry.BackOffExecutor;
import io.lettuce.core.LettuceFutures;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.codec.ByteArrayCodec;
import java.util.List;
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
  private List<RedisFuture> futures = Lists.newArrayList();

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
    }
  }

  @Override
  public boolean isConnected() {
    return connection != null;
  }

  @Override
  public void sync() {
    // Wait for some time for futures to complete
    // TODO: should this be configurable?
    try {
      LettuceFutures.awaitAll(60, TimeUnit.SECONDS, futures.toArray(new RedisFuture[0]));
    } finally {
      futures.clear();
    }
  }

  @Override
  public void pexpire(byte[] key, Long expiryMillis) {
    commands.pexpire(key, expiryMillis);
  }

  @Override
  public void append(byte[] key, byte[] value) {
    futures.add(commands.append(key, value));
  }

  @Override
  public void set(byte[] key, byte[] value) {
    futures.add(commands.set(key, value));
  }

  @Override
  public void lpush(byte[] key, byte[] value) {
    futures.add(commands.lpush(key, value));
  }

  @Override
  public void rpush(byte[] key, byte[] value) {
    futures.add(commands.rpush(key, value));
  }

  @Override
  public void sadd(byte[] key, byte[] value) {
    futures.add(commands.sadd(key, value));
  }

  @Override
  public void zadd(byte[] key, Long score, byte[] value) {
    futures.add(commands.zadd(key, score, value));
  }
}
