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
package feast.store.serving.redis;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.TransactionResult;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.codec.ByteArrayCodec;
import java.util.concurrent.ExecutionException;

public class LettuceTransactionPipeline {

  private StatefulRedisConnection<byte[], byte[]> connection;
  private RedisAsyncCommands<byte[], byte[]> commands;

  protected LettuceTransactionPipeline(RedisClient redisClient) {
    connection = redisClient.connect(new ByteArrayCodec());
    this.commands = connection.async();
  }

  public void clear() {
    this.commands.discard();
  }

  RedisFuture pexpire(byte[] k, long duration) {
    return commands.pexpire(k, duration);
  }

  void exec() throws ExecutionException, InterruptedException {
    RedisFuture<TransactionResult> exec = commands.exec();
    exec.get();
  }

  RedisFuture multi() {
    return this.commands.multi();
  }

  RedisFuture append(byte[] k, byte[] v) {
    return this.commands.append(k, v);
  }

  RedisFuture set(byte[] k, byte[] v) {
    return this.commands.set(k, v);
  }

  RedisFuture lpush(byte[] k, byte[] v) {
    return this.commands.lpush(k, v);
  }

  RedisFuture rpush(byte[] k, byte[] v) {
    return this.commands.rpush(k, v);
  }

  RedisFuture sadd(byte[] k, byte[] v) {
    return this.commands.sadd(k, v);
  }

  RedisFuture zadd(byte[] k, long s, byte[] v) {
    return this.commands.zadd(k, s, v);
  }

  void close() {
    this.clear();
    this.connection.close();
  }
}
