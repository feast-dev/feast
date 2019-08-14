/*
 * Copyright 2018 The Feast Authors
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
 *
 */

package feast.ingestion.util;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.gson.Gson;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.util.JsonFormat;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class ProtoUtil {

  private ProtoUtil() {}

  public static <T extends Message> T decodeProtoYamlFile(Path path, T prototype)
      throws IOException {
    String yaml = String.join("\n", Files.readAllLines(path));
    return decodeProtoYaml(yaml, prototype);
  }

  public static Message createProtoMessageFromYaml(String filePath, Message.Builder builder)
      throws IOException {
    String yamlString = new String(Files.readAllBytes(Paths.get(filePath)), StandardCharsets.UTF_8);
    ObjectMapper yamlReader = new ObjectMapper(new YAMLFactory());
    Object obj = yamlReader.readValue(yamlString, Object.class);

    ObjectMapper jsonWriter = new ObjectMapper();
    String jsonString = jsonWriter.writeValueAsString(obj);
    JsonFormat.parser().merge(jsonString, builder);
    return builder.build();
  }

  public static <T extends Message> T decodeProtoYaml(String yamlString, T prototype)
      throws IOException {
    ObjectMapper yamlMapper = new ObjectMapper(new YAMLFactory());
    ObjectMap map = yamlMapper.readerFor(ObjectMap.class).readValue(yamlString);
    ObjectMapper jsonMapper = new ObjectMapper(new JsonFactory());
    String json = jsonMapper.writerFor(ObjectMap.class).writeValueAsString(map);
    return decodeProtoJson(json, prototype);
  }

  public static <T extends Message> T decodeProtoJson(String jsonString, T prototype)
      throws IOException {
    T.Builder builder = prototype.newBuilderForType();
    JsonFormat.parser().merge(jsonString, builder);
    //noinspection unchecked
    return (T) builder.build();
  }

  public static <T extends Message> String encodeProtoJson(T message)
      throws InvalidProtocolBufferException {
    return JsonFormat.printer().omittingInsignificantWhitespace().print(message);
  }

  public static <T extends Message> String encodeProtoYaml(T message) {
    try {
      String json = encodeProtoJson(message);
      ObjectMap objectMap = new Gson().fromJson(json, ObjectMap.class);
      ObjectMapper yamlMapper = new ObjectMapper(new YAMLFactory());
      return yamlMapper.writer().writeValueAsString(objectMap);
    } catch (JsonProcessingException | InvalidProtocolBufferException e) {
      throw new RuntimeException(e);
    }
  }
}
