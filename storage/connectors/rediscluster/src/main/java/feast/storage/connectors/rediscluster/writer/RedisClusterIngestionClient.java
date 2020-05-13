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
package feast.storage.connectors.rediscluster.writer;

import com.google.common.collect.Lists;
import feast.core.StoreProto;
import feast.storage.common.retry.BackOffExecutor;
import io.lettuce.core.LettuceFutures;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.RedisURI;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.cluster.api.async.RedisAdvancedClusterAsyncCommands;
import io.lettuce.core.codec.ByteArrayCodec;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.joda.time.Duration;

public class RedisClusterIngestionClient implements RedisIngestionClient {

  private final BackOffExecutor backOffExecutor;
  private final List<RedisURI> uriList;
  private transient RedisClusterClient clusterClient;
  private StatefulRedisClusterConnection<byte[], byte[]> connection;
  private RedisAdvancedClusterAsyncCommands<byte[], byte[]> commands;
  private List<RedisFuture> futures = Lists.newArrayList();

  public RedisClusterIngestionClient(StoreProto.Store.RedisClusterConfig redisClusterConfig) {
    this.uriList =
        Arrays.stream(redisClusterConfig.getConnectionString().split(","))
            .map(
                hostPort -> {
                  String[] hostPortSplit = hostPort.trim().split(":");
                  return RedisURI.create(hostPortSplit[0], Integer.parseInt(hostPortSplit[1]));
                })
            .collect(Collectors.toList());

    long backoffMs =
        redisClusterConfig.getInitialBackoffMs() > 0 ? redisClusterConfig.getInitialBackoffMs() : 1;
    this.backOffExecutor =
        new BackOffExecutor(redisClusterConfig.getMaxRetries(), Duration.millis(backoffMs));
    this.clusterClient = RedisClusterClient.create(uriList);
  }

  @Override
  public void setup() {
    this.clusterClient = RedisClusterClient.create(this.uriList);
  }

  @Override
  public BackOffExecutor getBackOffExecutor() {
    return this.backOffExecutor;
  }

  @Override
  public void shutdown() {
    this.clusterClient.shutdown();
  }

  @Override
  public void connect() {
    if (!isConnected()) {
      this.connection = clusterClient.connect(new ByteArrayCodec());
      this.commands = connection.async();
    }
  }

  @Override
  public boolean isConnected() {
    return this.connection != null;
  }

  @Override
  public void sync() {
    try {
      LettuceFutures.awaitAll(60, TimeUnit.SECONDS, futures.toArray(new RedisFuture[0]));
    } finally {
      futures.clear();
    }
  }

  @Override
  public void pexpire(byte[] key, Long expiryMillis) {
    futures.add(commands.pexpire(key, expiryMillis));
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
