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

package feast.storage.bigtable;

import com.google.common.base.Charsets;
import com.google.common.collect.Lists;
import feast.SerializableCache;
import feast.ingestion.model.Specs;
import feast.ingestion.util.DateUtil;
import feast.options.OptionsParser;
import feast.specs.FeatureSpecProto.FeatureSpec;
import feast.storage.BigTableProto.BigTableRowKey;
import feast.types.FeatureProto.Feature;
import feast.types.FeatureRowExtendedProto.FeatureRowExtended;
import feast.types.FeatureRowProto.FeatureRow;
import feast.types.GranularityProto.Granularity;
import feast.types.GranularityProto.Granularity.Enum;
import java.util.Collections;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;

/**
 * DoFn for taking a feature row and making Bigtable mutations out of it. Also keys the mutations by
 * the entity name which should be used as the table name.
 */
@Slf4j
public class FeatureRowToBigTableMutationDoFn
    extends DoFn<FeatureRowExtended, KV<String, Mutation>> {

  private final SerializableCache<FeatureSpec, BigTableFeatureOptions> servingOptionsCache =
      SerializableCache.<FeatureSpec, BigTableFeatureOptions>builder()
          .loadingFunction(
              (featureSpec) ->
                  OptionsParser.parse(
                      featureSpec.getDataStores().getServing().getOptionsMap(),
                      BigTableFeatureOptions.class))
          .build();
  private final String tablePrefix;
  private final Specs specs;

  FeatureRowToBigTableMutationDoFn(String tablePrefix, Specs specs) {
    this.tablePrefix = tablePrefix;
    this.specs = specs;
  }

  public static BigTableRowKey makeBigTableRowKey(
      String entityKey, com.google.protobuf.Timestamp timestamp, Granularity.Enum granularity) {

    return BigTableRowKey.newBuilder()
        .setSha1Prefix(DigestUtils.sha1Hex(entityKey).substring(0, 7))
        .setEntityKey(entityKey)
        .setReversedMillis(String.valueOf(getReversedRoundedMillis(timestamp, granularity)))
        .build();
  }

  public static long getReversedRoundedMillis(
      com.google.protobuf.Timestamp timestamp, Granularity.Enum granularity) {
    if (granularity == Granularity.Enum.NONE) {
      return 0L; // We store it as zero instead of reversed Long.MAX_VALUE.
    }
    timestamp = DateUtil.roundToGranularity(timestamp, granularity);
    return Long.MAX_VALUE - timestamp.getSeconds() * 1000;
  }

  @ProcessElement
  public void processElement(ProcessContext context) {
    FeatureRowExtended rowExtended = context.element();
    FeatureRow row = rowExtended.getRow();
    List<Put> mutations = makePut(rowExtended);
    for (Put put : mutations) {
      context.output(KV.of(getTableName(row), put));
    }
  }

  private String getTableName(FeatureRow row) {
    if (tablePrefix != null) {
      return tablePrefix + row.getEntityName();
    } else {
      return row.getEntityName();
    }
  }

  /**
   * Given an row and a feature info service, build a BigTable Put mutation
   *
   * <p>bigtable row key = {sha1(row.key), row.key, row.timestamp} family = {feature.group,
   * granularity} qualifier = {feature.name} value = {feature.value}
   */
  public List<Put> makePut(FeatureRowExtended rowExtended) {
    FeatureRow row = rowExtended.getRow();
    Granularity.Enum granularity = row.getGranularity();
    // We always additinally overwrite a None granularity row so that it is trivial to retrieve the
    // latest across all features.
    Put latestPut =
        new Put(
            makeBigTableRowKey(row.getEntityKey(), row.getEventTimestamp(), Enum.NONE)
                .toByteArray());

    Put timeseriesPut =
        new Put(
            makeBigTableRowKey(row.getEntityKey(), row.getEventTimestamp(), granularity)
                .toByteArray());
    boolean isTimeseries = granularity.getNumber() != Enum.NONE.getNumber();

    // keep track of addColumn operation in case there is no mutation needed
    int mutationCount = 0;
    for (Feature feature : row.getFeaturesList()) {
      FeatureSpec featureSpec = specs.tryGetFeatureSpec(feature.getId());
      if (featureSpec == null) {
        continue;
      }

      BigTableFeatureOptions options = servingOptionsCache.get(featureSpec);

      byte[] family = options.family.getBytes(Charsets.UTF_8);
      byte[] qualifier = feature.getId().getBytes(Charsets.UTF_8);
      byte[] value = feature.getValue().toByteArray();
      // Note version will always the same for row key.
      long version = DateUtil.toMillis(row.getEventTimestamp());
      latestPut.addColumn(family, qualifier, version, value);
      if (isTimeseries) {
        timeseriesPut.addColumn(family, qualifier, version, value);
      }

      mutationCount++;
    }

    if (mutationCount == 0) {
      return Collections.emptyList();
    }

    if (isTimeseries) {
      return Lists.newArrayList(timeseriesPut, latestPut);
    } else {
      return Lists.newArrayList(latestPut);
    }
  }
}
