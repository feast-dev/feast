/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright 2018-2021 The Feast Authors
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
package feast.storage.connectors.sstable.retriever;

import com.google.common.hash.Hashing;
import feast.proto.serving.ServingAPIProto.FeatureReferenceV2;
import feast.proto.serving.ServingAPIProto.GetOnlineFeaturesRequestV2.EntityRow;
import feast.proto.types.ValueProto;
import feast.storage.api.retriever.Feature;
import feast.storage.api.retriever.OnlineRetrieverV2;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @param <K> Decoded value type of the partition key
 * @param <V> Type of the SSTable row
 */
public interface SSTableOnlineRetriever<K, V> extends OnlineRetrieverV2 {

  int MAX_TABLE_NAME_LENGTH = 50;

  @Override
  default List<List<Feature>> getOnlineFeatures(
      String project,
      List<EntityRow> entityRows,
      List<FeatureReferenceV2> featureReferences,
      List<String> entityNames) {

    List<String> columnFamilies = getSSTableColumns(featureReferences);
    String tableName = getSSTable(project, entityNames);

    List<K> rowKeys =
        entityRows.stream()
            .map(row -> convertEntityValueToKey(row, entityNames))
            .collect(Collectors.toList());

    Map<K, V> rowsFromSSTable = getFeaturesFromSSTable(tableName, rowKeys, columnFamilies);

    return convertRowToFeature(tableName, rowKeys, rowsFromSSTable, featureReferences);
  }

  /**
   * Generate SSTable key.
   *
   * @param entityRow Single EntityRow representation in feature retrieval call
   * @param entityNames List of entities related to feature references in retrieval call
   * @return SSTable key for retrieval
   */
  K convertEntityValueToKey(EntityRow entityRow, List<String> entityNames);

  /**
   * Converts SSTable rows into @NativeFeature type.
   *
   * @param tableName Name of SSTable
   * @param rowKeys List of keys of rows to retrieve
   * @param rows Map of rowKey to Row related to it
   * @param featureReferences List of feature references
   * @return List of List of Features associated with respective rowKey
   */
  List<List<Feature>> convertRowToFeature(
      String tableName,
      List<K> rowKeys,
      Map<K, V> rows,
      List<FeatureReferenceV2> featureReferences);

  /**
   * Retrieve rows for each row entity key.
   *
   * @param tableName Name of SSTable
   * @param rowKeys List of keys of rows to retrieve
   * @param columnFamilies List of column names
   * @return Map of retrieved features for each rowKey
   */
  Map<K, V> getFeaturesFromSSTable(String tableName, List<K> rowKeys, List<String> columnFamilies);

  /**
   * Retrieve name of SSTable corresponding to entities in retrieval call
   *
   * @param project Name of Feast project
   * @param entityNames List of entities used in retrieval call
   * @return Name of Cassandra table
   */
  default String getSSTable(String project, List<String> entityNames) {
    return trimAndHash(
        String.format("%s__%s", project, String.join("__", entityNames)), MAX_TABLE_NAME_LENGTH);
  }

  /**
   * Convert Entity value from Feast valueType to String type. Currently only supports STRING_VAL,
   * INT64_VAL, INT32_VAL and BYTES_VAL.
   *
   * @param v Entity value of Feast valueType
   * @return String representation of Entity value
   */
  default String valueToString(ValueProto.Value v) {
    String stringRepr;
    switch (v.getValCase()) {
      case STRING_VAL:
        stringRepr = v.getStringVal();
        break;
      case INT64_VAL:
        stringRepr = String.valueOf(v.getInt64Val());
        break;
      case INT32_VAL:
        stringRepr = String.valueOf(v.getInt32Val());
        break;
      case BYTES_VAL:
        stringRepr = v.getBytesVal().toString();
        break;
      default:
        throw new RuntimeException("Type is not supported to be entity");
    }

    return stringRepr;
  }

  /**
   * Retrieve SSTable columns based on Feature references.
   *
   * @param featureReferences List of feature references in retrieval call
   * @return List of String of column names
   */
  default List<String> getSSTableColumns(List<FeatureReferenceV2> featureReferences) {
    return featureReferences.stream()
        .map(FeatureReferenceV2::getFeatureTable)
        .distinct()
        .collect(Collectors.toList());
  }

  /**
   * Trims long SSTable table names and appends hash suffix for uniqueness.
   *
   * @param expr Original SSTable table name
   * @param maxLength Maximum length allowed for SSTable
   * @return Hashed suffix SSTable table name
   */
  default String trimAndHash(String expr, int maxLength) {
    // Length 8 as derived from murmurhash_32 implementation
    int maxPrefixLength = maxLength - 8;
    String finalName = expr;
    if (expr.length() > maxLength) {
      String hashSuffix =
          Hashing.murmur3_32().hashBytes(expr.substring(maxPrefixLength).getBytes()).toString();
      finalName = expr.substring(0, Math.min(expr.length(), maxPrefixLength)).concat(hashSuffix);
    }
    return finalName;
  }
}
