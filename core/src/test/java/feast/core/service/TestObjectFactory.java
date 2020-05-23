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
package feast.core.service;

import feast.core.SourceProto;
import feast.core.model.FeatureSet;
import feast.core.model.Field;
import feast.core.model.Source;
import feast.types.ValueProto;
import java.util.HashMap;
import java.util.List;

public class TestObjectFactory {

  public static Source defaultSource =
      new Source(
          SourceProto.SourceType.KAFKA,
          SourceProto.KafkaSourceConfig.newBuilder()
              .setBootstrapServers("kafka:9092")
              .setTopic("my-topic")
              .build(),
          true);

  public static FeatureSet CreateFeatureSet(
      String name, int version, List<Field> entities, List<Field> features) {
    return new FeatureSet(name, version, 100L, entities, features, defaultSource, new HashMap<>());
  }

  public static Field CreateFeatureField(
      String featureSetId, String name, ValueProto.ValueType.Enum valueType) {
    return new Field(featureSetId, name, valueType, new HashMap<String, String>());
  }

  public static Field CreateEntityField(
      String featureSetId, String name, ValueProto.ValueType.Enum valueType) {
    return new Field(featureSetId, name, valueType);
  }
}
