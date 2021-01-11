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

import feast.proto.core.StoreProto;
import io.lettuce.core.KeyValue;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.codec.ByteArrayCodec;
import java.util.List;

public class RedisClient implements RedisClientAdapter {

  private final RedisAsyncCommands<byte[], byte[]> asyncCommands;

  @Override
  public RedisFuture<List<KeyValue<byte[], byte[]>>> hmget(byte[] key, byte[]... fields) {
    return asyncCommands.hmget(key, fields);
  }

  @Override
  public void flushCommands() {
    asyncCommands.flushCommands();
  }

  private RedisClient(StatefulRedisConnection<byte[], byte[]> connection) {
    this.asyncCommands = connection.async();

    // Disable auto-flushing
    this.asyncCommands.setAutoFlushCommands(false);
  }

  public static RedisClientAdapter create(StoreProto.Store.RedisConfig config) {

    RedisURI uri = RedisURI.create(config.getHost(), config.getPort());

    if (config.getSsl()) {
      uri.setSsl(true);
    }
    StatefulRedisConnection<byte[], byte[]> connection =
        io.lettuce.core.RedisClient.create(uri).connect(new ByteArrayCodec());

    return new RedisClient(connection);
  }
}
