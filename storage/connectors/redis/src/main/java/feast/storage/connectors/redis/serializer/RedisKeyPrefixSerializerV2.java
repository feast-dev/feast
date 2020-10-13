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
package feast.storage.connectors.redis.serializer;

import feast.proto.storage.RedisProto.RedisKeyV2;

public class RedisKeyPrefixSerializerV2 implements RedisKeySerializerV2 {

  private final byte[] prefixBytes;

  public RedisKeyPrefixSerializerV2(String prefix) {
    this.prefixBytes = prefix.getBytes();
  }

  public byte[] serialize(RedisKeyV2 redisKey) {
    byte[] key = redisKey.toByteArray();

    if (prefixBytes.length == 0) {
      return key;
    }

    byte[] keyWithPrefix = new byte[prefixBytes.length + key.length];
    System.arraycopy(prefixBytes, 0, keyWithPrefix, 0, prefixBytes.length);
    System.arraycopy(key, 0, keyWithPrefix, prefixBytes.length, key.length);
    return keyWithPrefix;
  }
}
