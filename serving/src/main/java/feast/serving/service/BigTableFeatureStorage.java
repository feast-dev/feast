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

import com.google.cloud.bigtable.hbase.BigtableConfiguration;
import com.google.common.base.Preconditions;
import com.google.protobuf.Timestamp;
import com.google.protobuf.util.Timestamps;
import feast.serving.exception.FeatureRetrievalException;
import feast.serving.model.FeatureValue;
import feast.specs.FeatureSpecProto.FeatureSpec;
import feast.specs.StorageSpecProto.StorageSpec;
import feast.storage.BigTableProto.BigTableRowKey;
import feast.types.ValueProto.Value;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;

/**
 * Connector to BigTable instance.
 */
@Slf4j
public class BigTableFeatureStorage implements FeatureStorage {

  public static final String TYPE = "bigtable";
  private static final String DEFAULT_COLUMN_FAMILY = "default";
  public static String OPT_BIGTABLE_PROJECT = "project";
  public static String OPT_BIGTABLE_INSTANCE = "instance";
  public static String OPT_BIGTABLE_TABLE_PREFIX = "tablePrefix";
  public static String STORAGE_OPT_BIGTABLE_COLUMN_FAMILY = "family";
  public static String FEATURE_OPT_BIGTABLE_COLUMN_FAMILY = "bigtable.family";

  private final StorageSpec storageSpec;
  private final BigTableConnectionFactory connectionFactory;
  private transient Connection connection;


  public BigTableFeatureStorage(StorageSpec storageSpec) {
    Preconditions.checkArgument(storageSpec.getType().equals(TYPE));
    this.storageSpec = storageSpec;
    this.connectionFactory = new BigTableConnectionFactory(storageSpec);
  }

  /**
   * For tests
   */
  public BigTableFeatureStorage(StorageSpec storageSpec,
      BigTableConnectionFactory connectionFactory) {
    Preconditions.checkArgument(storageSpec.getType().equals(TYPE));
    this.storageSpec = storageSpec;
    this.connectionFactory = connectionFactory;
  }

  protected Connection getConnection() {
    if (connection == null) {
      connection = connectionFactory.connect();
    }
    return connection;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public List<FeatureValue> getFeature(
      String entityName, Collection<String> entityIds, Collection<FeatureSpec> featureSpecs) {
    List<FeatureValue> featureValues = new ArrayList<>(entityIds.size() * featureSpecs.size());
    for (FeatureSpec featureSpec : featureSpecs) {
      featureValues.addAll(getCurrentFeatureInternal(entityName, entityIds, featureSpec));
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
      String entityName, Collection<String> entityIds, FeatureSpec featureSpec) {
    List<FeatureValue> features = new ArrayList<>(entityIds.size());
    String featureId = featureSpec.getId();
    byte[] featureIdBytes = featureSpec.getId().getBytes();
    List<Get> gets = createGets(entityIds, featureSpec);
    try (Table table = getConnection().getTable(TableName.valueOf(entityName))) {
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
   * Create list of get operation for retrieving a feature of several entities optionally filtered
   * by its timestamp.
   *
   * @param entityIds list of entity ID.
   * @param featureSpec feature spec
   * @return list of Get operation.
   */
  private List<Get> createGets(Collection<String> entityIds, FeatureSpec featureSpec) {
    byte[] featureIdBytes = featureSpec.getId().getBytes();
    byte[] columnFamily = getColumnFamily(featureSpec);
    List<Get> gets = new ArrayList<>();
    for (String entityId : entityIds) {
      String entityIdPrefix = DigestUtils.sha1Hex(entityId.getBytes()).substring(0, 7);
      BigTableRowKey btKey = createRowKey(entityIdPrefix, entityId, "0");
      Get get = new Get(btKey.toByteArray());
      get.addColumn(columnFamily, featureIdBytes);
      try {
        get.readVersions(1);
      } catch (IOException e) {
        log.error("should not happen");
      }
      gets.add(get);
    }
    return gets;
  }

  /**
   * Create BigTableRowKey based on entityId, and timestamp.
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
   * Get column family of a feature from its spec.
   *
   * @param fs feature's spec
   * @return byte array value of the column family.
   */
  private byte[] getColumnFamily(FeatureSpec fs) {
    String family =
        fs.getOptionsOrDefault(FEATURE_OPT_BIGTABLE_COLUMN_FAMILY, null);
    if (family == null) {
      family = storageSpec
          .getOptionsOrDefault(STORAGE_OPT_BIGTABLE_COLUMN_FAMILY, DEFAULT_COLUMN_FAMILY);
    }
    return family.getBytes();
  }

  @AllArgsConstructor
  public static class BigTableConnectionFactory {

    private StorageSpec storageSpec;

    Connection connect() {
      return BigtableConfiguration.connect(
          storageSpec.getOptionsOrThrow(BigTableFeatureStorage.OPT_BIGTABLE_PROJECT),
          storageSpec.getOptionsOrThrow(BigTableFeatureStorage.OPT_BIGTABLE_INSTANCE));
    }
  }
}
