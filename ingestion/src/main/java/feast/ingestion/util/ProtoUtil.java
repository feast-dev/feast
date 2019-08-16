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
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.gson.Gson;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.Message.Builder;
import com.google.protobuf.util.JsonFormat;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
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

  public static String readStringFromUri(String uriPath) throws URISyntaxException, IOException {
    return readStringFromUri(uriPath, StorageOptions.getDefaultInstance().getService());
  }

  public static String readStringFromUri(String uriPath, Storage storage)
      throws URISyntaxException, IOException {
    URI uri = new URI(uriPath);
    String out = "";

    switch (uri.getScheme()) {
      case "file":
        if (uri.getHost() != null) {
          throw new IllegalArgumentException(
              "Please provide an 'absolute' path for a local file URI, for example use 'file:///tmp/myfile.txt' and NOT 'file://tmp/myfile.txt'. Invalid URI: "
                  + uriPath);
        }
        if (uri.getPath().endsWith("/")) {
          throw new IllegalArgumentException(
              "Please provide a URI to a local file NOT a directory. Invalid URI: " + uriPath);
        }
        out = new String(Files.readAllBytes(Paths.get(uri.getPath())), StandardCharsets.UTF_8);
        break;
      case "gs":
        String bucketName = uri.getHost();
        if (bucketName == null || bucketName.isEmpty()) {
          throw new IllegalArgumentException(
              "Missing bucket in the URI, expected URI in this pattern 'gs://<bucket>/<blob>'. Invalid URI: "
                  + uriPath);
        }
        if (uri.getPath() == null || uri.getPath().isEmpty()) {
          throw new IllegalArgumentException(
              "Missing blob in the URI, expected URI in this pattern 'gs://<bucket>/<blob>'. Invalid URI: "
                  + uriPath);
        }
        if (uri.getPath().endsWith("/")) {
          throw new IllegalArgumentException(
              "Invalid blob in the URI, should not end with a slash, expected URI in this pattern 'gs://<bucket>/<blob>'. Invalid URI: "
                  + uriPath);
        }
        String blobName = uri.getPath().substring(1);
        Blob blob = storage.get(bucketName, blobName);
        if (blob == null) {
          throw new IllegalArgumentException("File not found. Please check your URI: " + uriPath);
        }
        out = new String(blob.getContent(), StandardCharsets.UTF_8);
        break;
      default:
        throw new IllegalArgumentException(
            String.format(
                "Failed to retrieve the YAML file because the file URI has an invalid scheme '%s'. File URI must start with either 'file://' or 'gs://'. Invalid URI: %s",
                uri.getScheme(), uriPath));
    }

    if (out.isEmpty()) {
      throw new IllegalArgumentException(
          String.format(
              "Cannot read any content from '%s', please check that the uri path is valid",
              uriPath));
    }

    return out;
  }

  public static <T extends Message> T createProtoMessageFromYamlFileUri(
      String fileUri, Builder builder, Class<T> type) throws URISyntaxException {
    URI uri = new URI(fileUri);

    // // Create an object from the yaml file
    // ObjectMapper yamlReader = new ObjectMapper(new YAMLFactory());
    // String yamlString = new String(Files.readAllBytes(Paths.get(filePath)),
    // StandardCharsets.UTF_8);
    // Object yamlObject = yamlReader.readValue(yamlString, Object.class);
    //
    // // Create a JSON string representation of the object
    // ObjectMapper jsonWriter = new ObjectMapper();
    // String jsonString = jsonWriter.writeValueAsString(yamlObject);
    //
    // // Use protobuf util to create a protobuf message with fields corresponding to the JSON
    // string
    // JsonFormat.parser().merge(jsonString, builder);
    // Message message = builder.build();
    // return type.cast(message);

    // TODO
    return null;
  }

  public static <T extends Message> T createProtoMessageFromYamlString(
      String yamlString, Builder builder, Class<T> type) throws IOException {
    // Create an object from the yaml string
    ObjectMapper yamlReader = new ObjectMapper(new YAMLFactory());
    Object yamlObject = yamlReader.readValue(yamlString, Object.class);

    // Create a JSON string representation of the object
    ObjectMapper jsonWriter = new ObjectMapper();
    String jsonString = jsonWriter.writeValueAsString(yamlObject);

    // Use protobuf util to create a protobuf message with fields corresponding to the JSON string
    JsonFormat.parser().merge(jsonString, builder);
    Message message = builder.build();
    return type.cast(message);
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
