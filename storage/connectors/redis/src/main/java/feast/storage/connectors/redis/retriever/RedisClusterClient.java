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
package feast.storage.connectors.redis.retriever;

import com.google.common.collect.ImmutableMap;
import feast.proto.core.StoreProto;
import feast.proto.core.StoreProto.Store.RedisClusterConfig;
import feast.storage.connectors.redis.serializer.RedisKeyPrefixSerializerV2;
import feast.storage.connectors.redis.serializer.RedisKeySerializerV2;
import io.lettuce.core.KeyValue;
import io.lettuce.core.ReadFrom;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.RedisURI;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.cluster.api.async.RedisAdvancedClusterAsyncCommands;
import io.lettuce.core.codec.ByteArrayCodec;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

public class RedisClusterClient implements RedisClientAdapter {

  private final RedisAdvancedClusterAsyncCommands<byte[], byte[]> asyncCommands;
  private final RedisKeySerializerV2 serializer;
  @Nullable private final RedisKeySerializerV2 fallbackSerializer;

  private static final Map<RedisClusterConfig.ReadFrom, ReadFrom> PROTO_TO_LETTUCE_TYPES =
      ImmutableMap.of(
          RedisClusterConfig.ReadFrom.MASTER, ReadFrom.MASTER,
          RedisClusterConfig.ReadFrom.MASTER_PREFERRED, ReadFrom.MASTER_PREFERRED,
          RedisClusterConfig.ReadFrom.REPLICA, ReadFrom.REPLICA,
          RedisClusterConfig.ReadFrom.REPLICA_PREFERRED, ReadFrom.REPLICA_PREFERRED);

  @Override
  public RedisFuture<List<KeyValue<byte[], byte[]>>> hmget(byte[] key, byte[]... fields) {
    return asyncCommands.hmget(key, fields);
  }

  @Override
  public void flushCommands() {
    asyncCommands.flushCommands();
  }

  static class Builder {
    private final StatefulRedisClusterConnection<byte[], byte[]> connection;
    private final RedisKeySerializerV2 serializer;
    @Nullable private RedisKeySerializerV2 fallbackSerializer;

    Builder(
        StatefulRedisClusterConnection<byte[], byte[]> connection,
        RedisKeySerializerV2 serializer) {
      this.connection = connection;
      this.serializer = serializer;
    }

    Builder withFallbackSerializer(RedisKeySerializerV2 fallbackSerializer) {
      this.fallbackSerializer = fallbackSerializer;
      return this;
    }

    RedisClusterClient build() {
      return new RedisClusterClient(this);
    }
  }

  private RedisClusterClient(Builder builder) {
    this.asyncCommands = builder.connection.async();
    this.serializer = builder.serializer;
    this.fallbackSerializer = builder.fallbackSerializer;

    // allows reading from replicas
    this.asyncCommands.readOnly();

    // Disable auto-flushing
    this.asyncCommands.setAutoFlushCommands(false);
  }

  public static RedisClientAdapter create(StoreProto.Store.RedisClusterConfig config) {
    List<RedisURI> redisURIList =
        Arrays.stream(config.getConnectionString().split(","))
            .map(
                hostPort -> {
                  String[] hostPortSplit = hostPort.trim().split(":");
                  return RedisURI.create(hostPortSplit[0], Integer.parseInt(hostPortSplit[1]));
                })
            .collect(Collectors.toList());
    StatefulRedisClusterConnection<byte[], byte[]> connection =
        io.lettuce.core.cluster.RedisClusterClient.create(redisURIList)
            .connect(new ByteArrayCodec());

    connection.setReadFrom(PROTO_TO_LETTUCE_TYPES.get(config.getReadFrom()));

    RedisKeySerializerV2 serializer = new RedisKeyPrefixSerializerV2(config.getKeyPrefix());

    Builder builder = new Builder(connection, serializer);

    if (config.getEnableFallback()) {
      RedisKeySerializerV2 fallbackSerializer =
          new RedisKeyPrefixSerializerV2(config.getKeyPrefix());
      builder = builder.withFallbackSerializer(fallbackSerializer);
    }

    return builder.build();
  }
}
