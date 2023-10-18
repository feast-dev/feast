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
import java.util.List;
import java.util.Map;

public interface RedisClientAdapter {
  RedisFuture<List<KeyValue<byte[], byte[]>>> hmget(byte[] key, byte[]... fields);

  RedisFuture<Map<byte[], byte[]>> hgetall(byte[] key);

  void flushCommands();
}
