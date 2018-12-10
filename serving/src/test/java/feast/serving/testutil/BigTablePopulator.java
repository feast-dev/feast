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

import com.google.protobuf.Duration;
import com.google.protobuf.Timestamp;
import com.google.protobuf.util.Durations;
import com.google.protobuf.util.Timestamps;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import feast.serving.util.TimeUtil;
import feast.specs.FeatureSpecProto.FeatureSpec;
import feast.storage.BigTableProto.BigTableRowKey;
import feast.types.GranularityProto.Granularity.Enum;
import feast.types.ValueProto.Value;
import feast.types.ValueProto.ValueType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static junit.framework.TestCase.fail;

/** Helper class to populate a BigTable instance with fake data. */
public class BigTablePopulator extends FeatureStoragePopulator {
  private final Connection connection;
  private static final byte[] DEFAULT_COLUMN_FAMILY = "default".getBytes();

  public BigTablePopulator(Connection connection) {
    this.connection = connection;
  }

  /**
   * Populate big table instance with fake data ranging between 'start' and 'end'. The data will be
   * dense, meaning for each entity the data will be available for each granularity units. (except
   * for granularity second, for which the data is available every 30 seconds)
   *
   * @param entityIds
   * @param featureSpecs
   * @param start
   * @param end
   */
  @Override
  public void populate(
      String entityName,
      Collection<String> entityIds,
      Collection<FeatureSpec> featureSpecs,
      Timestamp start,
      Timestamp end) {

    createTableIfNecessary(entityName);
    populateTableWithFakeData(entityName, entityIds, featureSpecs, start, end);
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
      Timestamp start,
      Timestamp end) {
    TableName tableName = TableName.valueOf(entityName);
    try (Table table = connection.getTable(tableName)) {
      for (FeatureSpec featureSpec : featureSpecs) {
        for (String entityId : entityIds) {
          Timestamp roundedStart =
              TimeUtil.roundFloorTimestamp(start, featureSpec.getGranularity());
          Timestamp roundedEnd = TimeUtil.roundFloorTimestamp(end, featureSpec.getGranularity());
          List<Put> puts = makeMultiplePut(entityId, featureSpec, roundedStart, roundedEnd);
          table.put(puts);
        }
      }

    } catch (IOException e) {
      System.out.println("unable to connect to table: " + entityName);
      e.printStackTrace();
      fail();
    }
  }

  private List<Put> makeMultiplePut(
      String entityId, FeatureSpec fs, Timestamp start, Timestamp end) {
    String featureId = fs.getId();
    List<Put> puts = new ArrayList<>();
    if (fs.getGranularity().equals(Enum.NONE)) {
      Value val = createValue(entityId, featureId, fs.getValueType());
      puts.add(makeSinglePutForGranularityNone(entityId, fs.getId(), val));
      return puts;
    }

    Duration timesteps = getTimestep(fs.getGranularity());
    for (Timestamp iter = end;
        Timestamps.compare(iter, start) > 0;
        iter = Timestamps.subtract(iter, timesteps)) {
      Timestamp roundedIter = TimeUtil.roundFloorTimestamp(iter, fs.getGranularity());
      if (Timestamps.compare(iter, end) == 0) {
        Value value = createValue(entityId, featureId, iter, fs.getValueType());
        BigTableRowKey rowKey =
            BigTableRowKey.newBuilder()
                .setEntityKey(entityId)
                .setReversedMillis("0")
                .setSha1Prefix(DigestUtils.sha1Hex(entityId.getBytes()).substring(0, 7))
                .build();
        Put put = new Put(rowKey.toByteArray());
        long ts = Timestamps.toMillis(roundedIter);
        put.addColumn(DEFAULT_COLUMN_FAMILY, fs.getId().getBytes(), ts, value.toByteArray());
        puts.add(put);
      }
      puts.add(makeSinglePut(entityId, fs, roundedIter));
    }

    return puts;
  }

  private Put makeSinglePut(String entityId, FeatureSpec fs, Timestamp roundedTimestamp) {
    BigTableRowKey rowKey =
        BigTableRowKey.newBuilder()
            .setEntityKey(entityId)
            .setReversedMillis(roundedReversedMillis(roundedTimestamp, fs.getGranularity()))
            .setSha1Prefix(DigestUtils.sha1Hex(entityId.getBytes()).substring(0, 7))
            .build();
    Put put = new Put(rowKey.toByteArray());
    long timestamp = Timestamps.toMillis(roundedTimestamp);
    Value val = createValue(entityId, fs.getId(), roundedTimestamp, fs.getValueType());
    put.addColumn(DEFAULT_COLUMN_FAMILY, fs.getId().getBytes(), timestamp, val.toByteArray());
    return put;
  }

  private Put makeSinglePutForGranularityNone(String entityId, String featureId, Value value) {
    BigTableRowKey rowKey =
        BigTableRowKey.newBuilder()
            .setEntityKey(entityId)
            .setReversedMillis("0")
            .setSha1Prefix(DigestUtils.sha1Hex(entityId.getBytes()).substring(0, 7))
            .build();
    Put put = new Put(rowKey.toByteArray());
    put.addColumn(DEFAULT_COLUMN_FAMILY, featureId.getBytes(), 0, value.toByteArray());
    return put;
  }

  String roundedReversedMillis(Timestamp ts, Enum granularity) {
    if (granularity.equals(Enum.NONE)) {
      return "0";
    }
    Timestamp roundedEnd = TimeUtil.roundFloorTimestamp(ts, granularity);
    return String.valueOf(Long.MAX_VALUE - roundedEnd.getSeconds() * 1000);
  }

  // use at least hourly time step since big table is slow
  @Override
  protected Duration getTimestep(Enum granularity) {
    switch (granularity) {
      case SECOND:
        return Durations.fromSeconds(60 * 60);
      case MINUTE:
        return Durations.fromSeconds(60 * 60);
      case HOUR:
        return Durations.fromSeconds(60 * 60);
      case DAY:
        return Durations.fromSeconds(24 * 60 * 60);
      default:
        throw new IllegalArgumentException("unsupported granularity: " + granularity);
    }
  }
}
