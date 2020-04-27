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
package feast.ingestion.utils;

import static feast.types.ValueProto.ValueType;

import com.google.cloud.bigquery.StandardSQLTypeName;
import feast.core.FeatureSetProto.FeatureSetSpec;
import feast.core.StoreProto.Store;
import feast.core.StoreProto.Store.StoreType;
import feast.storage.api.writer.FeatureSink;
import feast.storage.connectors.bigquery.writer.BigQueryFeatureSink;
import feast.storage.connectors.redis.writer.RedisFeatureSink;
import feast.types.ValueProto.ValueType.Enum;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;

// TODO: Create partitioned table by default

/**
 * This class is a utility to manage storage backends in Feast.
 *
 * <p>Examples when schemas need to be updated:
 *
 * <ul>
 *   <li>when a new entity is registered, a table usually needs to be created
 *   <li>when a new feature is registered, a column with appropriate data type usually needs to be
 *       created
 * </ul>
 *
 * <p>If the storage backend is a key-value or a schema-less database, however, there may not be a
 * need to manage any schemas. This class will not be used in that case.
 */
public class StoreUtil {

  private static final Map<ValueType.Enum, StandardSQLTypeName> VALUE_TYPE_TO_STANDARD_SQL_TYPE =
      new HashMap<>();
  private static final Logger log = org.slf4j.LoggerFactory.getLogger(StoreUtil.class);

  // Column description for reserved fields
  public static final String BIGQUERY_EVENT_TIMESTAMP_FIELD_DESCRIPTION =
      "Event time for the FeatureRow";
  public static final String BIGQUERY_CREATED_TIMESTAMP_FIELD_DESCRIPTION =
      "Processing time of the FeatureRow ingestion in Feast\"";
  public static final String BIGQUERY_JOB_ID_FIELD_DESCRIPTION =
      "Feast import job ID for the FeatureRow";

  // Refer to protos/feast/core/Store.proto for the mapping definition.
  static {
    VALUE_TYPE_TO_STANDARD_SQL_TYPE.put(Enum.BYTES, StandardSQLTypeName.BYTES);
    VALUE_TYPE_TO_STANDARD_SQL_TYPE.put(Enum.STRING, StandardSQLTypeName.STRING);
    VALUE_TYPE_TO_STANDARD_SQL_TYPE.put(ValueType.Enum.INT32, StandardSQLTypeName.INT64);
    VALUE_TYPE_TO_STANDARD_SQL_TYPE.put(ValueType.Enum.INT64, StandardSQLTypeName.INT64);
    VALUE_TYPE_TO_STANDARD_SQL_TYPE.put(ValueType.Enum.DOUBLE, StandardSQLTypeName.FLOAT64);
    VALUE_TYPE_TO_STANDARD_SQL_TYPE.put(ValueType.Enum.FLOAT, StandardSQLTypeName.FLOAT64);
    VALUE_TYPE_TO_STANDARD_SQL_TYPE.put(ValueType.Enum.BOOL, StandardSQLTypeName.BOOL);
    VALUE_TYPE_TO_STANDARD_SQL_TYPE.put(Enum.BYTES_LIST, StandardSQLTypeName.BYTES);
    VALUE_TYPE_TO_STANDARD_SQL_TYPE.put(Enum.STRING_LIST, StandardSQLTypeName.STRING);
    VALUE_TYPE_TO_STANDARD_SQL_TYPE.put(Enum.INT32_LIST, StandardSQLTypeName.INT64);
    VALUE_TYPE_TO_STANDARD_SQL_TYPE.put(Enum.INT64_LIST, StandardSQLTypeName.INT64);
    VALUE_TYPE_TO_STANDARD_SQL_TYPE.put(Enum.DOUBLE_LIST, StandardSQLTypeName.FLOAT64);
    VALUE_TYPE_TO_STANDARD_SQL_TYPE.put(Enum.FLOAT_LIST, StandardSQLTypeName.FLOAT64);
    VALUE_TYPE_TO_STANDARD_SQL_TYPE.put(Enum.BOOL_LIST, StandardSQLTypeName.BOOL);
  }

  public static FeatureSink getFeatureSink(
      Store store, Map<String, FeatureSetSpec> featureSetSpecs) {
    StoreType storeType = store.getType();
    switch (storeType) {
      case REDIS:
        return RedisFeatureSink.fromConfig(store.getRedisConfig(), featureSetSpecs);
      case BIGQUERY:
        return BigQueryFeatureSink.fromConfig(store.getBigqueryConfig(), featureSetSpecs);
      default:
        throw new RuntimeException(String.format("Store type '{}' is unsupported", storeType));
    }
  }
}
