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
package feast.serving.connectors.redis.retriever;

import io.lettuce.core.*;
import io.lettuce.core.cluster.ClusterClientOptions;
import io.lettuce.core.cluster.ClusterTopologyRefreshOptions;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.cluster.api.async.RedisAdvancedClusterAsyncCommands;
import io.lettuce.core.codec.ByteArrayCodec;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class RedisClusterClient implements RedisClientAdapter {

  private final RedisAdvancedClusterAsyncCommands<byte[], byte[]> asyncCommands;

  @Override
  public RedisFuture<List<KeyValue<byte[], byte[]>>> hmget(byte[] key, byte[]... fields) {
    return asyncCommands.hmget(key, fields);
  }

  @Override
  public RedisFuture<Map<byte[], byte[]>> hgetall(byte[] key) {
    return asyncCommands.hgetall(key);
  }

  @Override
  public void flushCommands() {
    asyncCommands.flushCommands();
  }

  static class Builder {
    private final StatefulRedisClusterConnection<byte[], byte[]> connection;

    Builder(StatefulRedisClusterConnection<byte[], byte[]> connection) {
      this.connection = connection;
    }

    RedisClusterClient build() {
      return new RedisClusterClient(this);
    }
  }

  private RedisClusterClient(Builder builder) {
    this.asyncCommands = builder.connection.async();

    // allows reading from replicas
    this.asyncCommands.readOnly();
  }

  public static RedisClientAdapter create(RedisClusterStoreConfig config) {
    List<RedisURI> redisURIList =
        Arrays.stream(config.getConnectionString().split(","))
            .map(
                hostPort -> {
                  String[] hostPortSplit = hostPort.trim().split(":");
                  RedisURI redisURI =
                      RedisURI.create(hostPortSplit[0], Integer.parseInt(hostPortSplit[1]));
                  if (!config.getPassword().isEmpty()) {
                    redisURI.setPassword(config.getPassword());
                  }
                  if (config.getSsl()) {
                    redisURI.setSsl(true);
                  }
                  redisURI.setTimeout(config.getTimeout());
                  return redisURI;
                })
            .collect(Collectors.toList());

    io.lettuce.core.cluster.RedisClusterClient client =
        io.lettuce.core.cluster.RedisClusterClient.create(redisURIList);
    client.setOptions(
        ClusterClientOptions.builder()
            .socketOptions(SocketOptions.builder().keepAlive(true).tcpNoDelay(true).build())
            .timeoutOptions(TimeoutOptions.enabled(config.getTimeout()))
            .pingBeforeActivateConnection(true)
            .topologyRefreshOptions(
                ClusterTopologyRefreshOptions.builder().enableAllAdaptiveRefreshTriggers().build())
            .build());

    StatefulRedisClusterConnection<byte[], byte[]> connection =
        client.connect(new ByteArrayCodec());
    connection.setReadFrom(config.getReadFrom());

    return new Builder(connection).build();
  }
}
