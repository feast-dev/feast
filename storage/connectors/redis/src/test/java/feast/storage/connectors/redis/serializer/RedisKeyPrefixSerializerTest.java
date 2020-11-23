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

import static org.junit.Assert.*;

import feast.proto.storage.RedisProto.RedisKeyV2;
import feast.proto.types.ValueProto;
import org.junit.Test;

public class RedisKeyPrefixSerializerTest {

  private RedisKeyV2 key =
      RedisKeyV2.newBuilder()
          .addEntityNames("entity1")
          .addEntityValues(ValueProto.Value.newBuilder().setInt64Val(1))
          .build();

  @Test
  public void shouldPrependKey() {
    RedisKeyPrefixSerializerV2 serializer = new RedisKeyPrefixSerializerV2("namespace:");
    String keyWithPrefix = new String(serializer.serialize(key));
    assertEquals(String.format("namespace:%s", new String(key.toByteArray())), keyWithPrefix);
  }

  @Test
  public void shouldNotPrependKeyIfEmptyString() {
    RedisKeyPrefixSerializerV2 serializer = new RedisKeyPrefixSerializerV2("");
    assertArrayEquals(key.toByteArray(), serializer.serialize(key));
  }
}
