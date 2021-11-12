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
package feast.storage.connectors.bigtable.retriever;

import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.models.Filters;
import com.google.cloud.bigtable.data.v2.models.Query;
import com.google.cloud.bigtable.data.v2.models.Row;
import com.google.cloud.bigtable.data.v2.models.RowCell;
import com.google.protobuf.ByteString;
import com.google.protobuf.Timestamp;
import feast.proto.serving.ServingAPIProto.FeatureReferenceV2;
import feast.proto.serving.ServingAPIProto.GetOnlineFeaturesRequestV2.EntityRow;
import feast.storage.api.retriever.AvroFeature;
import feast.storage.api.retriever.Feature;
import feast.storage.connectors.sstable.retriever.SSTableOnlineRetriever;
import java.io.IOException;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.avro.AvroRuntimeException;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.*;

public class BigTableOnlineRetriever implements SSTableOnlineRetriever<ByteString, Row> {

  private BigtableDataClient client;
  private BigTableSchemaRegistry schemaRegistry;

  public BigTableOnlineRetriever(BigtableDataClient client) {
    this.client = client;
    this.schemaRegistry = new BigTableSchemaRegistry(client);
  }

  /**
   * Generate BigTable key in the form of entity values joined by #.
   *
   * @param entityRow Single EntityRow representation in feature retrieval call
   * @param entityNames List of entities related to feature references in retrieval call
   * @return BigTable key for retrieval
   */
  @Override
  public ByteString convertEntityValueToKey(EntityRow entityRow, List<String> entityNames) {
    return ByteString.copyFrom(
        entityNames.stream()
            .map(entity -> entityRow.getFieldsMap().get(entity))
            .map(this::valueToString)
            .collect(Collectors.joining("#"))
            .getBytes());
  }

  /**
   * Converts rowCell feature value into @NativeFeature type.
   *
   * @param tableName Name of BigTable table
   * @param rowKeys List of keys of rows to retrieve
   * @param rows Map of rowKey to Row related to it
   * @param featureReferences List of feature references
   * @return List of List of Features associated with respective rowKey
   */
  @Override
  public List<List<Feature>> convertRowToFeature(
      String tableName,
      List<ByteString> rowKeys,
      Map<ByteString, Row> rows,
      List<FeatureReferenceV2> featureReferences) {

    BinaryDecoder reusedDecoder = DecoderFactory.get().binaryDecoder(new byte[0], null);

    return rowKeys.stream()
        .map(
            rowKey -> {
              if (!rows.containsKey(rowKey)) {
                return Collections.<Feature>emptyList();
              } else {
                Row row = rows.get(rowKey);
                return featureReferences.stream()
                    .map(FeatureReferenceV2::getFeatureTable)
                    .distinct()
                    .map(cf -> row.getCells(cf, ""))
                    .filter(ls -> !ls.isEmpty())
                    .flatMap(
                        rowCells -> {
                          RowCell rowCell = rowCells.get(0); // Latest cell
                          String family = rowCell.getFamily();
                          ByteString value = rowCell.getValue();

                          List<Feature> features;
                          List<FeatureReferenceV2> localFeatureReferences =
                              featureReferences.stream()
                                  .filter(
                                      featureReference ->
                                          featureReference.getFeatureTable().equals(family))
                                  .collect(Collectors.toList());

                          try {
                            features =
                                decodeFeatures(
                                    tableName,
                                    value,
                                    localFeatureReferences,
                                    reusedDecoder,
                                    rowCell.getTimestamp());
                          } catch (IOException e) {
                            throw new RuntimeException("Failed to decode features from BigTable");
                          }

                          return features.stream();
                        })
                    .collect(Collectors.toList());
              }
            })
        .collect(Collectors.toList());
  }

  /**
   * Retrieve rows for each row entity key by generating BigTable rowQuery with filters based on
   * column families.
   *
   * @param tableName Name of BigTable table
   * @param rowKeys List of keys of rows to retrieve
   * @param columnFamilies List of FeatureTable names
   * @return Map of retrieved features for each rowKey
   */
  @Override
  public Map<ByteString, Row> getFeaturesFromSSTable(
      String tableName, List<ByteString> rowKeys, List<String> columnFamilies) {
    Query rowQuery = Query.create(tableName);
    Filters.InterleaveFilter familyFilter = Filters.FILTERS.interleave();
    columnFamilies.forEach(cf -> familyFilter.filter(Filters.FILTERS.family().exactMatch(cf)));

    for (ByteString rowKey : rowKeys) {
      rowQuery.rowKey(rowKey);
    }

    return StreamSupport.stream(client.readRows(rowQuery).spliterator(), false)
        .collect(Collectors.toMap(Row::getKey, Function.identity()));
  }

  /**
   * AvroRuntimeException is thrown if feature name does not exist in avro schema. Empty Object is
   * returned when null is retrieved from BigTable RowCell.
   *
   * @param tableName Name of BigTable table
   * @param value Value of BigTable cell where first 4 bytes represent the schema reference and
   *     remaining bytes represent avro-serialized features
   * @param featureReferences List of feature references
   * @param reusedDecoder Decoder for decoding feature values
   * @param timestamp Timestamp of rowCell
   * @return @NativeFeature with retrieved value stored in BigTable RowCell
   * @throws IOException
   */
  private List<Feature> decodeFeatures(
      String tableName,
      ByteString value,
      List<FeatureReferenceV2> featureReferences,
      BinaryDecoder reusedDecoder,
      long timestamp)
      throws IOException {
    ByteString schemaReferenceBytes = value.substring(0, 4);
    byte[] featureValueBytes = value.substring(4).toByteArray();

    BigTableSchemaRegistry.SchemaReference schemaReference =
        new BigTableSchemaRegistry.SchemaReference(tableName, schemaReferenceBytes);

    GenericDatumReader<GenericRecord> reader = schemaRegistry.getReader(schemaReference);

    reusedDecoder = DecoderFactory.get().binaryDecoder(featureValueBytes, reusedDecoder);
    GenericRecord record = reader.read(null, reusedDecoder);

    return featureReferences.stream()
        .map(
            featureReference -> {
              Object featureValue;
              try {
                featureValue = record.get(featureReference.getName());
              } catch (AvroRuntimeException e) {
                // Feature is not found in schema
                return null;
              }
              if (featureValue != null) {
                return new AvroFeature(
                    featureReference,
                    Timestamp.newBuilder().setSeconds(timestamp / 1000).build(),
                    featureValue);
              }
              return new AvroFeature(
                  featureReference,
                  Timestamp.newBuilder().setSeconds(timestamp / 1000).build(),
                  new Object());
            })
        .filter(Objects::nonNull)
        .collect(Collectors.toList());
  }
}
