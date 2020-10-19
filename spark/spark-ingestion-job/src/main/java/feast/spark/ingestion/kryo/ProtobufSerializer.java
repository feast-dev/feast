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
package feast.spark.ingestion.kryo;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.google.protobuf.AbstractMessage;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

/** Inspired by ProtobufSerializer in https://github.com/magro/kryo-serializers */
public class ProtobufSerializer<T extends AbstractMessage> extends Serializer<T> {

  private final Method parseFromMethod;

  public ProtobufSerializer(Class<T> type) {
    super();
    try {
      this.parseFromMethod = type.getMethod("parseFrom", byte[].class);
    } catch (NoSuchMethodException e) {
      throw new IllegalArgumentException(
          "Cannot resolve method parseFrom(byte[]) in class " + type.getName());
    }
  }

  @Override
  public void write(Kryo kryo, Output output, T object) {
    if (object == null) {
      output.write(0);
      output.flush();
      return;
    }

    byte[] bytes = object.toByteArray();

    output.writeInt(bytes.length);

    output.writeBytes(bytes);
    output.flush();
  }

  @Override
  public T read(Kryo kryo, Input input, Class<T> type) {
    int length = input.readInt();

    if (length == 0) {
      return null;
    }

    byte[] bytes = input.readBytes(length);
    try {
      return (T) parseFromMethod.invoke(type, bytes);
    } catch (InvocationTargetException | IllegalAccessException e) {
      throw new RuntimeException("Unable to deserialize protobuf " + e.getMessage(), e);
    }
  }

  public boolean getAcceptsNull() {
    return true;
  }
}
