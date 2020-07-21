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

import feast.proto.types.ValueProto.ValueType;

public class StatsType {
  // Category of statistics a feature falls into. Determines the set of
  // statistics to generate for the feature.
  enum Enum {
    NUMERIC,
    CATEGORICAL,
    BYTES,
    LIST,
  }

  /**
   * Returns the category of feature statistics to build for the given valueType.
   *
   * @param valueType {@link ValueType.Enum} of a feature
   * @return {@link StatsType.Enum} corresponding to the given valueType.
   */
  public static StatsType.Enum fromValueType(ValueType.Enum valueType) {
    switch (valueType) {
      case FLOAT:
      case DOUBLE:
      case INT32:
      case INT64:
      case BOOL:
        return Enum.NUMERIC;
      case STRING:
        return Enum.CATEGORICAL;
      case BYTES:
        return Enum.BYTES;
      case BYTES_LIST:
      case BOOL_LIST:
      case FLOAT_LIST:
      case INT32_LIST:
      case INT64_LIST:
      case DOUBLE_LIST:
      case STRING_LIST:
        return Enum.LIST;
      default:
        throw new IllegalArgumentException(
            String.format("Invalid feature type provided: %s", valueType));
    }
  }
}
