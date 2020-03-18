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

import feast.storage.common.retry.BackOffExecutor;
import java.io.Serializable;

public interface RedisIngestionClient extends Serializable {

  void setup();

  BackOffExecutor getBackOffExecutor();

  void shutdown();

  void connect();

  boolean isConnected();

  void sync();

  void pexpire(byte[] key, Long expiryMillis);

  void append(byte[] key, byte[] value);

  void set(byte[] key, byte[] value);

  void lpush(byte[] key, byte[] value);

  void rpush(byte[] key, byte[] value);

  void sadd(byte[] key, byte[] value);

  void zadd(byte[] key, Long score, byte[] value);
}
