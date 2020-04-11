/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright 2018-2019 The Feast Authors
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
package feast.serving.util.mappers;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.protobuf.util.JsonFormat;
import feast.core.StoreProto;
import feast.core.StoreProto.Store;
import feast.core.StoreProto.Store.Builder;
import java.io.IOException;
import org.slf4j.Logger;

public class YamlToProtoMapper {

  private static final Logger log = org.slf4j.LoggerFactory.getLogger(YamlToProtoMapper.class);

  private static final ObjectMapper yamlReader = new ObjectMapper(new YAMLFactory());
  private static final ObjectMapper jsonWriter = new ObjectMapper();

  public static Store yamlToStoreProto(String yaml) {
    try {
      Object obj = yamlReader.readValue(yaml, Object.class);
      String jsonString = jsonWriter.writeValueAsString(obj);
      Builder builder = Store.newBuilder();
      JsonFormat.parser().merge(jsonString, builder);
      return builder.build();
    } catch (IOException e) {
      log.error("Could not parse store configuration YAML", e);
      return StoreProto.Store.getDefaultInstance();
    }
  }
}
