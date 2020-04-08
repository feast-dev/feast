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
package feast.storage.connectors.bigquery.statistics;

import feast.core.FeatureSetProto.EntitySpec;
import feast.core.FeatureSetProto.FeatureSpec;
import feast.types.ValueProto.ValueType.Enum;

/**
 * Value class for Features containing information necessary to template stats-retrieving queries.
 */
public class FieldStatisticsQueryInfo {
  private final String name;
  private final String type;

  private FieldStatisticsQueryInfo(String name, String type) {
    this.name = name;
    this.type = type;
  }

  public static FieldStatisticsQueryInfo fromProto(FeatureSpec featureSpec) {
    Enum valueType = featureSpec.getValueType();
    switch (valueType) {
      case FLOAT:
      case DOUBLE:
      case INT32:
      case INT64:
      case BOOL:
        return new FieldStatisticsQueryInfo(featureSpec.getName(), "NUMERIC");
      case STRING:
        return new FieldStatisticsQueryInfo(featureSpec.getName(), "CATEGORICAL");
      case BYTES:
        return new FieldStatisticsQueryInfo(featureSpec.getName(), "BYTES");
      case BYTES_LIST:
      case BOOL_LIST:
      case FLOAT_LIST:
      case INT32_LIST:
      case INT64_LIST:
      case DOUBLE_LIST:
      case STRING_LIST:
        return new FieldStatisticsQueryInfo(featureSpec.getName(), "LIST");
      default:
        throw new IllegalArgumentException("Invalid feature type provided");
    }
  }

  public static FieldStatisticsQueryInfo fromProto(EntitySpec entitySpec) {
    Enum valueType = entitySpec.getValueType();
    switch (valueType) {
      case FLOAT:
      case DOUBLE:
      case INT32:
      case INT64:
      case BOOL:
        return new FieldStatisticsQueryInfo(entitySpec.getName(), "NUMERIC");
      case STRING:
        return new FieldStatisticsQueryInfo(entitySpec.getName(), "CATEGORICAL");
      case BYTES:
        return new FieldStatisticsQueryInfo(entitySpec.getName(), "BYTES");
      case BYTES_LIST:
      case BOOL_LIST:
      case FLOAT_LIST:
      case INT32_LIST:
      case INT64_LIST:
      case DOUBLE_LIST:
      case STRING_LIST:
        return new FieldStatisticsQueryInfo(entitySpec.getName(), "LIST");
      default:
        throw new IllegalArgumentException("Invalid entity type provided");
    }
  }

  public String getName() {
    return name;
  }

  public String getType() {
    return type;
  }
}
