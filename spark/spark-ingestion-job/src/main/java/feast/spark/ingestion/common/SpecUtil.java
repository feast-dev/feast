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
package feast.spark.ingestion.common;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import feast.proto.core.FeatureSetProto;
import feast.proto.core.IngestionJobProto;
import feast.proto.core.SourceProto;
import feast.proto.core.StoreProto;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.tuple.Pair;

public class SpecUtil {

  public static String PROJECT_DEFAULT_NAME = "default";

  public static Pair<String, String> parseFeatureSetReference(String reference) {
    String[] split = reference.split("/", 2);
    if (split.length == 1) {
      return Pair.of(PROJECT_DEFAULT_NAME, split[0]);
    } else {
      return Pair.of(split[0], split[1]);
    }
  }

  public static List<StoreProto.Store> parseStoreJsonList(List<String> jsonList) {
    List<StoreProto.Store> stores = new ArrayList<>();
    for (String json : jsonList) {
      StoreProto.Store.Builder builder = StoreProto.Store.newBuilder();
      try {
        JsonFormat.parser().merge(json, builder);
      } catch (InvalidProtocolBufferException e) {
        throw new RuntimeException(
            String.format("Couldn't parse StoreProto.Store object from json: %s", e.getCause()));
      }
      stores.add(builder.build());
    }
    return stores;
  }

  public static SourceProto.Source parseSourceJson(String jsonSource) {
    SourceProto.Source.Builder builder = SourceProto.Source.newBuilder();
    try {
      JsonFormat.parser().merge(jsonSource, builder);
    } catch (InvalidProtocolBufferException e) {
      throw new RuntimeException(
          String.format("Couldn't parse SourceProto.Source object from json: %s", e.getCause()));
    }

    return builder.build();
  }

  public static IngestionJobProto.SpecsStreamingUpdateConfig parseSpecsStreamingUpdateConfig(
      String jsonConfig) throws InvalidProtocolBufferException {
    IngestionJobProto.SpecsStreamingUpdateConfig.Builder builder =
        IngestionJobProto.SpecsStreamingUpdateConfig.newBuilder();
    JsonFormat.parser().merge(jsonConfig, builder);
    return builder.build();
  }

  public static Map<String, Field> getFieldsByName(FeatureSetProto.FeatureSetSpec featureSetSpec) {
    Map<String, Field> fieldByName = new HashMap<>();
    for (FeatureSetProto.EntitySpec entitySpec : featureSetSpec.getEntitiesList()) {
      fieldByName.put(
          entitySpec.getName(), new Field(entitySpec.getName(), entitySpec.getValueType()));
    }
    for (FeatureSetProto.FeatureSpec featureSpec : featureSetSpec.getFeaturesList()) {
      fieldByName.put(
          featureSpec.getName(), new Field(featureSpec.getName(), featureSpec.getValueType()));
    }
    return fieldByName;
  }
}
