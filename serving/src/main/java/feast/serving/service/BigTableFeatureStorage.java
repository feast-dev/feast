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

package feast.serving.service;

import com.google.common.base.Strings;
import com.google.protobuf.Timestamp;
import com.google.protobuf.util.Timestamps;
import feast.serving.ServingAPIProto.TimestampRange;
import feast.serving.exception.FeatureRetrievalException;
import feast.serving.model.FeatureValue;
import feast.serving.model.Pair;
import feast.serving.util.TimeUtil;
import feast.specs.FeatureSpecProto.FeatureSpec;
import feast.storage.BigTableProto.BigTableRowKey;
import feast.types.GranularityProto.Granularity;
import feast.types.ValueProto.Value;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.MultiRowRangeFilter;
import org.apache.hadoop.hbase.filter.MultiRowRangeFilter.RowRange;

/** Connector to BigTable instance. */
@Slf4j
public class BigTableFeatureStorage implements FeatureStorage {

  public static final String TYPE = "bigtable";

  public static String OPT_BIGTABLE_PROJECT = "project";
  public static String OPT_BIGTABLE_INSTANCE = "instance";
  public static String OPT_BIGTABLE_TABLE_PREFIX = "tablePrefix";

  public static String SERVING_OPT_BIGTABLE_COLUMN_FAMILY = "family";

  private final Connection connection;

  private static final byte[] DEFAULT_COLUMN_FAMILY = "default".getBytes();

  public BigTableFeatureStorage(Connection connection) {
    this.connection = connection;
  }

  /** {@inheritDoc} */
  @Override
  public List<FeatureValue> getCurrentFeature(
      String entityName, List<String> entityIds, FeatureSpec featureSpec)
      throws FeatureRetrievalException {
    return getCurrentFeatureInternal(entityName, entityIds, featureSpec);
  }

  /** {@inheritDoc} */
  @Override
  public List<FeatureValue> getCurrentFeatures(
      String entityName, List<String> entityIds, List<FeatureSpec> featureSpecs) {
    List<FeatureValue> featureValues = new ArrayList<>(entityIds.size() * featureSpecs.size());
    for (FeatureSpec featureSpec : featureSpecs) {
      featureValues.addAll(getCurrentFeatureInternal(entityName, entityIds, featureSpec));
    }
    return featureValues;
  }

  /** {@inheritDoc} */
  @Override
  public List<FeatureValue> getNLatestFeatureWithinTimestampRange(
      String entityName,
      List<String> entityIds,
      Pair<FeatureSpec, Integer> featureSpecLimitPair,
      TimestampRange tsRange)
      throws FeatureRetrievalException {
    FeatureSpec featureSpec = featureSpecLimitPair.getLeft();
    int limit = featureSpecLimitPair.getRight();

    return getNLatestFeatureWithinTimestampRangeInternal(
        entityName, entityIds, featureSpec, limit, tsRange);
  }

  /** {@inheritDoc} */
  @Override
  public List<FeatureValue> getNLatestFeaturesWithinTimestampRange(
      String entityName,
      List<String> entityIds,
      List<Pair<FeatureSpec, Integer>> featureSpecAndLimitPairs,
      TimestampRange tsRange) {
    List<FeatureValue> featureValues = new ArrayList<>(entityIds.size() * featureSpecAndLimitPairs.size());
    for(Pair<FeatureSpec, Integer> featureSpecLimitPair: featureSpecAndLimitPairs) {
      FeatureSpec featureSpec = featureSpecLimitPair.getLeft();
      int limit = featureSpecLimitPair.getRight();

      featureValues.addAll(getNLatestFeatureWithinTimestampRangeInternal(
          entityName, entityIds, featureSpec, limit, tsRange));
    }
    return featureValues;
  }

  /**
   * Internal implementation of get current value of a feature for a list of entity Ids.
   *
   * @param entityName entity name.
   * @param entityIds list of entity id.
   * @param featureSpec spec of the feature.
   * @return list of feature value.
   */
  private List<FeatureValue> getCurrentFeatureInternal(
      String entityName, List<String> entityIds, FeatureSpec featureSpec) {
    List<FeatureValue> features = new ArrayList<>(entityIds.size());
    String featureId = featureSpec.getId();
    byte[] featureIdBytes = featureSpec.getId().getBytes();
    List<Get> gets = createGets(entityIds, featureIdBytes, getColumnFamily(featureSpec));
    try (Table table = connection.getTable(TableName.valueOf(entityName))) {
      Result[] results = table.get(gets);
      for (Result result : results) {
        Cell currentCell = result.getColumnLatestCell(getColumnFamily(featureSpec), featureIdBytes);
        if (currentCell == null) {
          continue;
        }

        byte[] rawRowKey = currentCell.getRowArray();
        if (rawRowKey == null) {
          continue;
        }

        BigTableRowKey rowKey = BigTableRowKey.parseFrom(rawRowKey);
        String entityId = rowKey.getEntityKey();
        byte[] rawCellValue = currentCell.getValueArray();

        if (rawCellValue == null) {
          continue;
        }

        Timestamp timestamp = Timestamps.fromMillis(currentCell.getTimestamp());
        Value value = Value.parseFrom(rawCellValue);
        FeatureValue featureValue = new FeatureValue(featureId, entityId, value, timestamp);
        features.add(featureValue);
      }
      return features;
    } catch (IOException e) {
      log.error("Error while retrieving feature from BigTable", e);
      throw new FeatureRetrievalException("Error while retrieving feature from BigTable", e);
    }
  }

  /**
   * Internal implementation of getting N latest feature within certain time range given a list of entity ID.
   *
   * @param entityName entity name
   * @param entityIds list of entity id for which the feature should be retrieved.
   * @param featureSpec spec of the feature.
   * @param limit maximum number of value to be retrieved per feature.
   * @param tsRange timestamp range.
   * @return list of feature value.
   */
  private List<FeatureValue> getNLatestFeatureWithinTimestampRangeInternal(
      String entityName,
      List<String> entityIds,
      FeatureSpec featureSpec,
      int limit,
      TimestampRange tsRange) {
    Scan scan = createScanFromFeatureSpec(featureSpec);
    Granularity.Enum granularity = featureSpec.getGranularity();
    String featureId = featureSpec.getId();
    byte[] featureIdBytes = featureSpec.getId().getBytes();
    String revMilliStart = roundedReversedMillis(tsRange.getStart(), granularity);
    String revMilliEnd = roundedReversedMillis(tsRange.getEnd(), granularity);
    scan.setFilter(createMultiRowRangeFilter(entityIds, revMilliStart, revMilliEnd));

    List<FeatureValue> features = new ArrayList<>();
    Map<String, Integer> featureCount = new HashMap<>(); // map of entity ID to number of feature.
    try (Table table = connection.getTable(TableName.valueOf(entityName));
        ResultScanner scanResult = table.getScanner(scan)) {
      for (Result row : scanResult) {
        Cell currentCell = row.getColumnLatestCell(getColumnFamily(featureSpec), featureIdBytes);
        if (currentCell == null) {
          continue;
        }

        BigTableRowKey rowKey = BigTableRowKey.parseFrom(currentCell.getRowArray());
        String entityId = rowKey.getEntityKey();
        byte[] cellValue = currentCell.getValueArray();

        if (cellValue == null) {
          continue;
        }

        Integer count = featureCount.computeIfAbsent(entityId, k -> 0);
        if (count < limit) {
          Timestamp timestamp = Timestamps.fromMillis(currentCell.getTimestamp());
          Value value = Value.parseFrom(cellValue);
          FeatureValue featureValue = new FeatureValue(featureId, entityId, value, timestamp);
          features.add(featureValue);

          count++;
          featureCount.put(entityId, count);
        }
      }

      return features;
    } catch (IOException e) {
      log.error("Error while retrieving feature: {}", e);
      throw new FeatureRetrievalException("Unable to retrieve feature from BigTable", e);
    }
  }

  /**
   * Create list of BigTable's Get operation.
   *
   * @param entityIds list of entity ID.
   * @param featureIdBytes byte array value of a feature ID.
   * @param columnFamily byte array value of column family
   * @return list of Get operation.
   */
  private List<Get> createGets(
      Collection<String> entityIds, byte[] featureIdBytes, byte[] columnFamily) {
    List<Get> gets = new ArrayList<>();
    for (String entityId : entityIds) {
      String entityIdPrefix = DigestUtils.sha1Hex(entityId.getBytes()).substring(0, 7);
      BigTableRowKey btKey = createRowKey(entityIdPrefix, entityId, "0");
      Get get = new Get(btKey.toByteArray());
      get.addColumn(columnFamily, featureIdBytes);
      try {
        // for some reason Get.readVersions has checked exception.
        get.readVersions(1);
      } catch (IOException e) {
        log.error("should not happen");
      }
      gets.add(get);
    }
    return gets;
  }

  /**
   * Create BigTable's scan operation for a certain feature.
   *
   * @param featureSpec spec of feature.
   * @return BigTable's scan operation.
   */
  private Scan createScanFromFeatureSpec(FeatureSpec featureSpec) {
    Scan scan = new Scan();
    byte[] columnFamily = getColumnFamily(featureSpec);
    scan.addColumn(columnFamily, featureSpec.getId().getBytes());
    scan.readVersions(1);
    return scan;
  }

  /**
   * Create BigTables's multi row range filter to scan several entity ID within a timerange
   *
   * @param entityIds list of entity id to be scanned.
   * @param reversedMilliStart starting timerange.
   * @param reversedMillisEnd end timerange.
   * @return instance of {@link MultiRowRangeFilter}.
   */
  private MultiRowRangeFilter createMultiRowRangeFilter(
      Collection<String> entityIds, String reversedMilliStart, String reversedMillisEnd) {
    List<RowRange> rowRangeList = new ArrayList<>();
    for (String entityId : entityIds) {
      String entityIdPrefix = DigestUtils.sha1Hex(entityId.getBytes()).substring(0, 7);
      BigTableRowKey startRow = createRowKey(entityIdPrefix, entityId, reversedMillisEnd);
      BigTableRowKey stopRow = createRowKey(entityIdPrefix, entityId, reversedMilliStart);
      rowRangeList.add(new RowRange(startRow.toByteArray(), true, stopRow.toByteArray(), true));
    }
    return new MultiRowRangeFilter(rowRangeList);
  }

  /**
   * Create BigTableRowKey based on entityId, timestamp, and granularity.
   *
   * @param entityIdPrefix hash prefix of entity ID.
   * @param entityId entity ID value
   * @param reversedMillisTimestamp reversed timestamp value.
   * @return instance of {@link BigTableRowKey} assocciated with the entity ID.
   */
  private BigTableRowKey createRowKey(
      String entityIdPrefix, String entityId, String reversedMillisTimestamp) {
    return BigTableRowKey.newBuilder()
        .setSha1Prefix(entityIdPrefix)
        .setEntityKey(entityId)
        .setReversedMillis(reversedMillisTimestamp)
        .build();
  }

  /**
   * Get rounded and reversed millis value of a timestamp.
   *
   * @param ts timestamp
   * @param granularity granularity of feature.
   * @return string value of timestamp after being rounded and reversed.
   */
  private String roundedReversedMillis(Timestamp ts, Granularity.Enum granularity) {
    if (granularity.equals(Granularity.Enum.NONE)) {
      return "0";
    }
    Timestamp roundedEnd = TimeUtil.roundFloorTimestamp(ts, granularity);
    return String.valueOf(Long.MAX_VALUE - roundedEnd.getSeconds() * 1000);
  }

  /**
   * Get column family of a feature from its spec.
   * @param fs feature's spec
   * @return byte array value of the column family.
   */
  private byte[] getColumnFamily(FeatureSpec fs) {
    String family =
        fs.getDataStores().getServing().getOptionsMap().get(SERVING_OPT_BIGTABLE_COLUMN_FAMILY);
    if (Strings.isNullOrEmpty(family)) {
      return DEFAULT_COLUMN_FAMILY;
    }
    return family.getBytes();
  }
}
