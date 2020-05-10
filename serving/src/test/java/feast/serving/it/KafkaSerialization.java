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
package feast.serving.it;

import com.google.protobuf.GeneratedMessageV3;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.Parser;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;

/*
Serializer & Deserializer implementation to write & read protobuf object from/to kafka
 */
public class KafkaSerialization {
  public static class ProtoSerializer<T extends Message> implements Serializer<T> {
    @Override
    public byte[] serialize(String topic, T data) {
      ByteArrayOutputStream stream = new ByteArrayOutputStream();
      try {
        data.writeTo(stream);
      } catch (IOException e) {
        throw new RuntimeException(
            String.format(
                "Unable to serialize object of type %s. Reason: %s",
                data.getClass().getName(), e.getCause().getMessage()));
      }
      return stream.toByteArray();
    }
  }

  public static class ProtoDeserializer<T extends GeneratedMessageV3> implements Deserializer<T> {
    private Parser<T> parser;

    public ProtoDeserializer(Parser<T> parser) {
      this.parser = parser;
    }

    @Override
    public T deserialize(String topic, byte[] data) {
      try {
        return parser.parseFrom(data);
      } catch (InvalidProtocolBufferException e) {
        throw new RuntimeException(
            String.format(
                "Unable to deserialize object from topic %s. Reason: %s",
                topic, e.getCause().getMessage()));
      }
    }
  }
}
