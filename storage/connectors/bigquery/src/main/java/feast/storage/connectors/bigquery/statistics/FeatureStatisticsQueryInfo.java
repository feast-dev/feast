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

import feast.proto.core.FeatureSetProto.FeatureSpec;
import feast.proto.types.ValueProto.ValueType;
import feast.proto.types.ValueProto.ValueType.Enum;
import java.util.HashMap;
import java.util.Map;
import org.tensorflow.metadata.v0.FeatureNameStatistics;
import org.tensorflow.metadata.v0.FeatureNameStatistics.Type;

/**
 * Value class for Features containing information necessary to template stats-retrieving queries.
 */
public class FeatureStatisticsQueryInfo {
  // Map converting Feast type to TFDV type
  private static final Map<Enum, Type> TFDV_TYPE_MAP = new HashMap<>();

  static {
    TFDV_TYPE_MAP.put(ValueType.Enum.INT64, FeatureNameStatistics.Type.INT);
    TFDV_TYPE_MAP.put(ValueType.Enum.INT32, FeatureNameStatistics.Type.INT);
    TFDV_TYPE_MAP.put(ValueType.Enum.BOOL, FeatureNameStatistics.Type.INT);
    TFDV_TYPE_MAP.put(ValueType.Enum.FLOAT, FeatureNameStatistics.Type.FLOAT);
    TFDV_TYPE_MAP.put(ValueType.Enum.DOUBLE, FeatureNameStatistics.Type.FLOAT);
    TFDV_TYPE_MAP.put(ValueType.Enum.STRING, FeatureNameStatistics.Type.STRING);
    TFDV_TYPE_MAP.put(ValueType.Enum.BYTES, FeatureNameStatistics.Type.BYTES);
    TFDV_TYPE_MAP.put(ValueType.Enum.BYTES_LIST, FeatureNameStatistics.Type.STRUCT);
    TFDV_TYPE_MAP.put(ValueType.Enum.STRING_LIST, FeatureNameStatistics.Type.STRUCT);
    TFDV_TYPE_MAP.put(ValueType.Enum.INT32_LIST, FeatureNameStatistics.Type.STRUCT);
    TFDV_TYPE_MAP.put(ValueType.Enum.INT64_LIST, FeatureNameStatistics.Type.STRUCT);
    TFDV_TYPE_MAP.put(ValueType.Enum.BOOL_LIST, FeatureNameStatistics.Type.STRUCT);
    TFDV_TYPE_MAP.put(ValueType.Enum.FLOAT_LIST, FeatureNameStatistics.Type.STRUCT);
    TFDV_TYPE_MAP.put(ValueType.Enum.DOUBLE_LIST, FeatureNameStatistics.Type.STRUCT);
  }

  // Name of the field
  private final String name;

  // Statistics Type to generate for the field
  private final String statsType;

  // Value Type of the field
  private final String valueType;

  private FeatureStatisticsQueryInfo(
      String name, StatsType.Enum statsType, FeatureNameStatistics.Type valueType) {
    this.name = name;
    this.statsType = statsType.toString();
    this.valueType = valueType.toString();
  }

  public static FeatureStatisticsQueryInfo fromProto(FeatureSpec featureSpec) {
    Enum valueType = featureSpec.getValueType();
    StatsType.Enum statsType = StatsType.fromValueType(valueType);
    return new FeatureStatisticsQueryInfo(
        featureSpec.getName(), statsType, TFDV_TYPE_MAP.get(valueType));
  }

  public String getName() {
    return name;
  }

  public String getStatsType() {
    return statsType;
  }

  public String getValueType() {
    return valueType;
  }
}
