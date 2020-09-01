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

import com.google.common.collect.Lists;
import feast.proto.storage.RedisProto.RedisKey;
import feast.proto.types.FieldProto;
import feast.proto.types.ValueProto;
import org.junit.Test;

public class RedisKeyPrefixSerializerTest {

  private RedisKey key =
      RedisKey.newBuilder()
          .setFeatureSet("project/featureSet")
          .addAllEntities(
              Lists.newArrayList(
                  FieldProto.Field.newBuilder()
                      .setName("entity1")
                      .setValue(ValueProto.Value.newBuilder().setInt64Val(1))
                      .build()))
          .build();

  @Test
  public void shouldPrependKey() {
    RedisKeyPrefixSerializer serializer = new RedisKeyPrefixSerializer("namespace:");
    String keyWithPrefix = new String(serializer.serialize(key));
    assertEquals(String.format("namespace:%s", new String(key.toByteArray())), keyWithPrefix);
  }

  @Test
  public void shouldNotPrependKeyIfEmptyString() {
    RedisKeyPrefixSerializer serializer = new RedisKeyPrefixSerializer("");
    assertArrayEquals(key.toByteArray(), serializer.serialize(key));
  }
}
