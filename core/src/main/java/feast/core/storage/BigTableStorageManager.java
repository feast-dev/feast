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

import com.google.cloud.bigtable.hbase.BigtableConfiguration;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import feast.core.log.Action;
import feast.core.log.AuditLogger;
import feast.core.log.Resource;
import feast.specs.FeatureSpecProto.FeatureSpec;
import feast.specs.StorageSpecProto.StorageSpec;
import java.io.IOException;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;

@Slf4j
public class BigTableStorageManager implements StorageManager {

  public static final String TYPE = "bigtable";
  public static final String STORE_OPT_BIGTABLE_PROJECT = "project";
  public static final String STORE_OPT_BIGTABLE_INSTANCE = "instance";
  public static final String STORE_OPT_BIGTABLE_TABLE_PREFIX = "tablePrefix";

  public static final String STORE_OPT_BIGTABLE_TABLE_COLUMN_FAMILY = "family";
  public static final String FEATURE_OPT_BIGTABLE_TABLE_COLUMN_FAMILY = "bigtable.family";
  private static final String DEFAULT_COLUMN_FAMILY = "default";

  private final StorageSpec storageSpec;
  private transient Connection connection;

  public BigTableStorageManager(StorageSpec storageSpec) {
    Preconditions.checkArgument(storageSpec.getType().equals(TYPE));
    this.storageSpec = storageSpec;
  }

  Connection getConnection() {
    if (connection == null) {
      String projectId = storageSpec.getOptionsOrThrow(STORE_OPT_BIGTABLE_PROJECT);
      String instanceId = storageSpec.getOptionsOrThrow(STORE_OPT_BIGTABLE_INSTANCE);
      connection = BigtableConfiguration.connect(projectId, instanceId);
    }
    return connection;
  }

  /**
   * Update the Bigtable schema given the addition of a new feature
   *
   * @param featureSpec specification of the new feature.
   */
  @Override
  public void registerNewFeature(FeatureSpec featureSpec) {
    String entityName = featureSpec.getEntity();
    String columnFamily = featureSpec
        .getOptionsOrDefault(FEATURE_OPT_BIGTABLE_TABLE_COLUMN_FAMILY, null);
    if (Strings.isNullOrEmpty(columnFamily)) {
      columnFamily = storageSpec
          .getOptionsOrDefault(STORE_OPT_BIGTABLE_TABLE_COLUMN_FAMILY, DEFAULT_COLUMN_FAMILY);
    }
    String tablePrefix = storageSpec.getOptionsOrDefault(STORE_OPT_BIGTABLE_TABLE_PREFIX, "");
    String tableNameString = tablePrefix + entityName;

    try (Admin admin = getConnection().getAdmin()) {
      TableName tableName = TableName.valueOf(tableNameString.getBytes());

      if (!admin.tableExists(tableName)) {
        TableDescriptor tableDescriptor = TableDescriptorBuilder.newBuilder(tableName).build();
        admin.createTable(tableDescriptor);
        log.info("Created new table for entity: {}", entityName);
      }

      TableDescriptor tableDescriptor = admin.getDescriptor(tableName);
      if (!isColumnFamilyExist(tableDescriptor, columnFamily)) {
        ColumnFamilyDescriptor cfDesc = ColumnFamilyDescriptorBuilder
            .newBuilder(columnFamily.getBytes())
            .build();

        admin.addColumnFamily(tableName, cfDesc);
        log.info("Created new column family: {} for entity: {}", columnFamily, entityName);
      }
      AuditLogger.log(
          Resource.STORAGE,
          storageSpec.getId(),
          Action.SCHEMA_UPDATE,
          "Bigtable schema updated for feature %s",
          featureSpec.getId());
    } catch (IOException e) {
      log.error("Unable to create table in BigTable: {}", e);
      throw new StorageInitializationException("Unable to create table in BigTable", e);
    }
  }

  private boolean isColumnFamilyExist(TableDescriptor tableDescriptor, String columnFamilyName) {
    return tableDescriptor.getColumnFamily(columnFamilyName.getBytes()) != null;
  }
}
