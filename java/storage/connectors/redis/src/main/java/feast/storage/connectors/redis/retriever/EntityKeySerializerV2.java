/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright 2018-2021 The Feast Authors
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

import com.google.protobuf.ProtocolStringList;
import feast.proto.storage.RedisProto;
import feast.proto.types.ValueProto;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.*;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.tuple.Pair;

// This is derived from
// https://github.com/feast-dev/feast/blob/b1ccf8dd1535f721aee8bea937ee38feff80bec5/sdk/python/feast/infra/key_encoding_utils.py#L22
// and must be kept up to date with any changes in that logic.
public class EntityKeySerializerV2 implements EntityKeySerializer {

  @Override
  public byte[] serialize(RedisProto.RedisKeyV2 entityKey) {
    final ProtocolStringList joinKeys = entityKey.getEntityNamesList();
    final List<ValueProto.Value> values = entityKey.getEntityValuesList();

    assert joinKeys.size() == values.size();

    final List<Byte> buffer = new ArrayList<>();

    final List<Pair<String, ValueProto.Value>> tuples = new ArrayList<>(joinKeys.size());
    for (int i = 0; i < joinKeys.size(); i++) {
      tuples.add(Pair.of(joinKeys.get(i), values.get(i)));
    }
    tuples.sort(Comparator.comparing(Pair::getLeft));

    for (Pair<String, ValueProto.Value> pair : tuples) {
      buffer.addAll(encodeInteger(ValueProto.ValueType.Enum.STRING.getNumber()));
      buffer.addAll(encodeString(pair.getLeft()));
    }

    for (Pair<String, ValueProto.Value> pair : tuples) {
      final ValueProto.Value val = pair.getRight();
      switch (val.getValCase()) {
        case STRING_VAL:
          String stringVal = val.getStringVal();

          buffer.addAll(encodeInteger(ValueProto.ValueType.Enum.STRING.getNumber()));
          buffer.addAll(encodeInteger(stringVal.length()));
          buffer.addAll(encodeString(stringVal));

          break;
        case BYTES_VAL:
          byte[] bytes = val.getBytesVal().toByteArray();

          buffer.addAll(encodeInteger(ValueProto.ValueType.Enum.BYTES.getNumber()));
          buffer.addAll(encodeInteger(bytes.length));
          buffer.addAll(encodeBytes(bytes));

          break;
        case INT32_VAL:
          buffer.addAll(encodeInteger(ValueProto.ValueType.Enum.INT32.getNumber()));
          buffer.addAll(encodeInteger(Integer.BYTES));
          buffer.addAll(encodeInteger(val.getInt32Val()));

          break;
        case INT64_VAL:
          buffer.addAll(encodeInteger(ValueProto.ValueType.Enum.INT64.getNumber()));
          buffer.addAll(encodeInteger(Integer.BYTES));
          /* This is super dumb - but in https://github.com/feast-dev/feast/blob/dcae1606f53028ce5413567fb8b66f92cfef0f8e/sdk/python/feast/infra/key_encoding_utils.py#L9
          we use `struct.pack("<l", v.int64_val)` to get the bytes of an int64 val. This actually extracts only 4 bytes,
          instead of 8 bytes as you'd expect from to serialize an int64 value.
          */
          buffer.addAll(encodeInteger(((Long) val.getInt64Val()).intValue()));

          break;
        default:
          throw new RuntimeException("Unable to serialize Entity Key");
      }
    }

    buffer.addAll(encodeString(entityKey.getProject()));

    final byte[] bytes = new byte[buffer.size()];
    for (int i = 0; i < buffer.size(); i++) {
      bytes[i] = buffer.get(i);
    }

    return bytes;
  }

  private List<Byte> encodeBytes(byte[] toByteArray) {
    return Arrays.asList(ArrayUtils.toObject(toByteArray));
  }

  private List<Byte> encodeInteger(Integer value) {
    ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES);
    buffer.order(ByteOrder.LITTLE_ENDIAN);
    buffer.putInt(value);

    return Arrays.asList(ArrayUtils.toObject(buffer.array()));
  }

  private List<Byte> encodeString(String value) {
    byte[] stringBytes = value.getBytes(StandardCharsets.UTF_8);
    return encodeBytes(stringBytes);
  }
}
