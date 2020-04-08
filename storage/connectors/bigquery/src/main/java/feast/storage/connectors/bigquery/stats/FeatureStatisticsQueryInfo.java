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
package feast.storage.connectors.bigquery.stats;

import feast.core.FeatureSetProto.FeatureSpec;
import feast.types.ValueProto.ValueType.Enum;

public class FeatureStatisticsQueryInfo {
  private final String name;
  private final String type;

  private FeatureStatisticsQueryInfo(String name, String type) {
    this.name = name;
    this.type = type;
  }

  public static FeatureStatisticsQueryInfo fromProto(FeatureSpec featureSpec) {
    Enum valueType = featureSpec.getValueType();
    switch (valueType) {
      case FLOAT:
      case DOUBLE:
      case INT32:
      case INT64:
      case BOOL:
        return new FeatureStatisticsQueryInfo(featureSpec.getName(), "NUMERIC");
      case STRING:
        return new FeatureStatisticsQueryInfo(featureSpec.getName(), "CATEGORICAL");
      case BYTES:
        return new FeatureStatisticsQueryInfo(featureSpec.getName(), "BYTES");
      case BYTES_LIST:
      case BOOL_LIST:
      case FLOAT_LIST:
      case INT32_LIST:
      case INT64_LIST:
      case DOUBLE_LIST:
      case STRING_LIST:
        return new FeatureStatisticsQueryInfo(featureSpec.getName(), "LIST");
      default:
        throw new IllegalArgumentException("Invalid feature type provided");
    }
  }

  public String getName() {
    return name;
  }

  public String getType() {
    return type;
  }
}
