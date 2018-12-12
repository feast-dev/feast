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

package feast.core.storage;

import feast.core.log.Action;
import feast.core.log.AuditLogger;
import feast.core.log.Resource;
import org.jdbi.v3.core.Jdbi;
import feast.specs.FeatureSpecProto.FeatureSpec;
import feast.types.ValueProto.ValueType.Enum;

import java.util.HashMap;
import java.util.Map;

public class PostgresStorageManager implements StorageManager {

  public static final String TYPE = "postgres";

  public static final String OPT_POSTGRES_URI = "uri"; // jdbc connection URI
  public static final String OPT_POSTGRES_TABLE_PREFIX = "tablePrefix";

  private String id;
  private final String connectionUri;
  private static final Map<Enum, String> FEAST_TO_POSTGRES_TYPE_MAP = new HashMap<>();

  static {
    FEAST_TO_POSTGRES_TYPE_MAP.put(Enum.BOOL, "BOOLEAN");
    FEAST_TO_POSTGRES_TYPE_MAP.put(Enum.INT32, "INTEGER");
    FEAST_TO_POSTGRES_TYPE_MAP.put(Enum.INT64, "BIGINT");
    FEAST_TO_POSTGRES_TYPE_MAP.put(Enum.BYTES, "VARBINARY");
    FEAST_TO_POSTGRES_TYPE_MAP.put(Enum.FLOAT, "REAL");
    FEAST_TO_POSTGRES_TYPE_MAP.put(Enum.DOUBLE, "DOUBLE");
    FEAST_TO_POSTGRES_TYPE_MAP.put(Enum.TIMESTAMP, "TIMESTAMP");
    FEAST_TO_POSTGRES_TYPE_MAP.put(Enum.STRING, "VARCHAR");
  }

  private static final String CREATE_TABLE_TEMPLATE =
          "CREATE TABLE IF NOT EXISTS %s ( "
                  + "id VARCHAR(255) PRIMARY KEY NOT NULL , timestamp TIMESTAMP )";
  private static final String INSERT_COLUMN_TEMPLATE =
          "ALTER TABLE %s ADD COLUMN IF NOT EXISTS %s %s";

  public PostgresStorageManager(String id, String connectionUri) {
    this.id = id;
    this.connectionUri = connectionUri;
  }

  /**
   * Update the schema of this table given the addition of this feature.
   * @param featureSpec specification of the new feature.
   */
  @Override
  public void registerNewFeature(FeatureSpec featureSpec) {
    String tableName = createTableName(featureSpec);
    String column = featureSpec.getName();
    String fieldType = createFieldType(featureSpec);

    Jdbi jdbi = Jdbi.create(connectionUri);
    jdbi.withHandle(
            handle -> {
              handle.execute(String.format(CREATE_TABLE_TEMPLATE, tableName));
              handle.execute(String.format(INSERT_COLUMN_TEMPLATE, tableName, column, fieldType));
              return null;
            });
    AuditLogger.log(
        Resource.STORAGE,
        this.id,
        Action.SCHEMA_UPDATE,
        "Postgres schema updated for feature %s",
        featureSpec.getId());
  }

  private String createFieldType(FeatureSpec featureSpec) {
    return FEAST_TO_POSTGRES_TYPE_MAP.get(featureSpec.getValueType());
  }

  private String createTableName(FeatureSpec featureSpec) {
    String entityName = featureSpec.getEntity().toLowerCase();
    String granularity = featureSpec.getGranularity().toString().toLowerCase();
    return String.format("%s_%s", entityName, granularity);
  }
}
