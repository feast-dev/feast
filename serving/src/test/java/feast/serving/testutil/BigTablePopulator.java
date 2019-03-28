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

package feast.serving.testutil;

import static junit.framework.TestCase.fail;

import com.google.protobuf.Timestamp;
import com.google.protobuf.util.Timestamps;
import feast.specs.FeatureSpecProto.FeatureSpec;
import feast.storage.BigTableProto.BigTableRowKey;
import feast.types.ValueProto.Value;
import java.io.IOException;
import java.util.Collection;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;

/**
 * Helper class to populate a BigTable instance with fake data. It mimic ingesting data to BigTable.
 */
public class BigTablePopulator extends FeatureStoragePopulator {
  private static final byte[] DEFAULT_COLUMN_FAMILY = "default".getBytes();
  private static final String LATEST_KEY = "0";
  private final Connection connection;

  public BigTablePopulator(Connection connection) {
    this.connection = connection;
  }

  /**
   * Populate big table instance with fake data.
   *
   * @param entityName entity name of the feature
   * @param entityIds collection of entity ID for which the feature should be populated
   * @param featureSpecs collection of feature specs for which the feature should be populated
   * @param timestamp timestamp of the features
   */
  @Override
  public void populate(
      String entityName,
      Collection<String> entityIds,
      Collection<FeatureSpec> featureSpecs,
      Timestamp timestamp) {

    createTableIfNecessary(entityName);
    populateTableWithFakeData(entityName, entityIds, featureSpecs, timestamp);
  }

  private void createTableIfNecessary(String entityName) {
    try (Admin admin = connection.getAdmin()) {

      TableName tableName = TableName.valueOf(entityName);
      TableDescriptor tableDescriptor = new HTableDescriptor(tableName);
      if (admin.tableExists(tableName)) {
        return;
      }

      admin.createTable(tableDescriptor);
      ColumnFamilyDescriptor cfDesc = ColumnFamilyDescriptorBuilder.of(DEFAULT_COLUMN_FAMILY);
      admin.addColumnFamily(tableName, cfDesc);

    } catch (TableExistsException e) {
      System.out.println("Table already exists: " + entityName);
    } catch (IOException e) {
      System.out.println("unable to connect to table: " + entityName);
      e.printStackTrace();
      fail();
    }
  }

  private void populateTableWithFakeData(
      String entityName,
      Collection<String> entityIds,
      Collection<FeatureSpec> featureSpecs,
      Timestamp timestamp) {
    TableName tableName = TableName.valueOf(entityName);
    try (Table table = connection.getTable(tableName)) {
      for (FeatureSpec featureSpec : featureSpecs) {
        for (String entityId : entityIds) {
          Put put = makePut(entityId, featureSpec, timestamp);
          table.put(put);
        }
      }

    } catch (IOException e) {
      System.out.println("unable to connect to table: " + entityName);
      e.printStackTrace();
      fail();
    }
  }

  private Put makePut(String entityId, FeatureSpec fs, Timestamp roundedTimestamp) {
    BigTableRowKey rowKey =
        BigTableRowKey.newBuilder()
            .setEntityKey(entityId)
            .setReversedMillis(LATEST_KEY)
            .setSha1Prefix(DigestUtils.sha1Hex(entityId.getBytes()).substring(0, 7))
            .build();
    Put put = new Put(rowKey.toByteArray());
    long timestamp = Timestamps.toMillis(roundedTimestamp);
    Value val = createValue(entityId, fs.getId(), roundedTimestamp, fs.getValueType());
    put.addColumn(DEFAULT_COLUMN_FAMILY, fs.getId().getBytes(), timestamp, val.toByteArray());
    return put;
  }
}
