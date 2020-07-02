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
package feast.storage.connectors.bigtable.writer;

import com.google.cloud.bigtable.beam.CloudBigtableIO;
import com.google.cloud.bigtable.beam.CloudBigtableTableConfiguration;
import com.google.common.collect.Iterators;
import feast.proto.core.FeatureSetProto.EntitySpec;
import feast.proto.core.FeatureSetProto.FeatureSetSpec;
import feast.proto.core.StoreProto.Store.BigtableConfig;
import feast.proto.storage.BigtableProto.BigtableKey;
import feast.proto.storage.BigtableProto.BigtableKey.Builder;
import feast.proto.types.FeatureRowProto.FeatureRow;
import feast.proto.types.FieldProto.Field;
import feast.storage.api.writer.FailedElement;
import feast.storage.api.writer.WriteResult;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.*;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BigtableCustomIO {

  private static final String METADATA_CF = "metadata";
  private static final String FEATURES_CF = "features";
  private static final String FEATURE_SET_QUALIFIER = "feature_set";
  private static final String INGESTION_ID_QUALIFIER = "ingestion_id";
  private static final String EVENT_TIMESTAMP_QUALIFIER = "event_timestamp";

  private static final TupleTag<Mutation> successfulMutationsTag =
      new TupleTag<Mutation>("successfulInserts") {};
  private static final TupleTag<FailedElement> failedInsertsTupleTag =
      new TupleTag<FailedElement>("failedInserts") {};

  private static final Logger log = LoggerFactory.getLogger(BigtableCustomIO.class);

  private BigtableCustomIO() {}

  public static Write write(
      BigtableConfig bigtableConfig,
      PCollectionView<Map<String, Iterable<FeatureSetSpec>>> featureSetSpecs) {
    return new Write(bigtableConfig, featureSetSpecs);
  }

  /** ServingStoreWrite data to a Bigtable server. */
  public static class Write extends PTransform<PCollection<FeatureRow>, WriteResult> {

    private final PCollectionView<Map<String, Iterable<FeatureSetSpec>>> featureSetSpecs;
    private final CloudBigtableTableConfiguration bigtableConfig;

    public Write(
        BigtableConfig bigtableConfig,
        PCollectionView<Map<String, Iterable<FeatureSetSpec>>> featureSetSpecs) {
      this.bigtableConfig =
          new CloudBigtableTableConfiguration.Builder()
              .withProjectId(bigtableConfig.getProjectId())
              .withInstanceId(bigtableConfig.getInstanceId())
              .withTableId(bigtableConfig.getTableId())
              .build();
      this.featureSetSpecs = featureSetSpecs;
    }

    @Override
    public WriteResult expand(PCollection<FeatureRow> input) {
      PCollectionTuple bigtableMutations =
          input.apply(
              ParDo.of(new MutationDoFn(featureSetSpecs))
                  .withOutputTags(successfulMutationsTag, TupleTagList.of(failedInsertsTupleTag))
                  .withSideInputs(featureSetSpecs));
      bigtableMutations
          .get(successfulMutationsTag)
          .apply(CloudBigtableIO.writeToTable(bigtableConfig));
      // Since BigQueryIO does not support emitting failure writes, we set failedElements to
      // an empty stream
      PCollection<FailedElement> failedElements =
          input
              .getPipeline()
              .apply(Create.of(""))
              .apply(
                  "dummy",
                  ParDo.of(
                      new DoFn<String, FailedElement>() {
                        @ProcessElement
                        public void processElement(ProcessContext context) {}
                      }));
      return WriteResult.in(input.getPipeline(), input, failedElements);
    }

    public static class MutationDoFn extends DoFn<FeatureRow, Mutation> {

      private final List<FeatureRow> featureRows = new ArrayList<>();
      private PCollectionView<Map<String, Iterable<FeatureSetSpec>>> featureSetSpecs;

      MutationDoFn(PCollectionView<Map<String, Iterable<FeatureSetSpec>>> featureSetSpecs) {
        this.featureSetSpecs = featureSetSpecs;
      }

      private FailedElement toFailedElement(
          FeatureRow featureRow, Exception exception, String jobName) {
        return FailedElement.newBuilder()
            .setJobName(jobName)
            .setTransformName("BigtableCustomIO")
            .setPayload(featureRow.toString())
            .setErrorMessage(exception.getMessage())
            .setStackTrace(ExceptionUtils.getStackTrace(exception))
            .build();
      }

      /**
       * private FailedElement toFailedElement( FeatureRow featureRow, Exception exception, String
       * jobName) { return FailedElement.newBuilder() .setJobName(jobName)
       * .setTransformName("BigtableCustomIO") .setPayload(featureRow.toString())
       * .setErrorMessage(exception.getMessage())
       * .setStackTrace(ExceptionUtils.getStackTrace(exception)) .build(); } *
       */
      private String getKey(FeatureRow featureRow, FeatureSetSpec featureSetSpec) {
        List<String> entityNames =
            featureSetSpec.getEntitiesList().stream()
                .map(EntitySpec::getName)
                .sorted()
                .collect(Collectors.toList());

        Map<String, Field> entityFields = new HashMap<>();
        Builder bigtableKeyBuilder =
            BigtableKey.newBuilder().setFeatureSet(featureRow.getFeatureSet());
        for (Field field : featureRow.getFieldsList()) {
          if (entityNames.contains(field.getName())) {
            entityFields.putIfAbsent(
                field.getName(),
                Field.newBuilder().setName(field.getName()).setValue(field.getValue()).build());
          }
        }
        for (String entityName : entityNames) {
          bigtableKeyBuilder.addEntities(entityFields.get(entityName));
        }
        BigtableKey btk = bigtableKeyBuilder.build();
        StringBuilder bigtableKey = new StringBuilder();
        for (Field field : btk.getEntitiesList()) {
          bigtableKey.append(field.getValue().getStringVal());
        }
        bigtableKey.append("#");
        for (Field field : btk.getEntitiesList()) {
          bigtableKey.append(field.getName());
          bigtableKey.append("#");
        }
        bigtableKey.append(featureSetSpec.getProject());
        bigtableKey.append("#");
        bigtableKey.append(featureSetSpec.getName());
        return bigtableKey.toString();
      }

      @ProcessElement
      public void processElement(ProcessContext context) {
        Map<String, FeatureSetSpec> latestSpecs =
            context.sideInput(featureSetSpecs).entrySet().stream()
                .map(e -> ImmutablePair.of(e.getKey(), Iterators.getLast(e.getValue().iterator())))
                .collect(Collectors.toMap(ImmutablePair::getLeft, ImmutablePair::getRight));
        FeatureRow featureRow = context.element();
        try {
          FeatureSetSpec featureSetSpec = latestSpecs.get(featureRow.getFeatureSet());
          String key = getKey(featureRow, featureSetSpec);
          log.info("Setting the key: {}", key);
          long timestamp = System.currentTimeMillis();
          Put row = new Put(Bytes.toBytes(key));
          row.addColumn(
              Bytes.toBytes(METADATA_CF),
              Bytes.toBytes(FEATURE_SET_QUALIFIER),
              timestamp,
              Bytes.toBytes(featureRow.getFeatureSet()));
          row.addColumn(
              Bytes.toBytes(METADATA_CF),
              Bytes.toBytes(INGESTION_ID_QUALIFIER),
              timestamp,
              Bytes.toBytes(featureRow.getIngestionId()));
          row.addColumn(
              Bytes.toBytes(METADATA_CF),
              Bytes.toBytes(EVENT_TIMESTAMP_QUALIFIER),
              timestamp,
              featureRow.getEventTimestamp().toByteArray());
          for (Field field : featureRow.getFieldsList()) {
            row.addColumn(
                Bytes.toBytes(FEATURES_CF),
                field.getName().getBytes(),
                timestamp,
                field.getValue().toByteArray());
          }
          context.output(successfulMutationsTag, row);
        } catch (Exception e) {
          featureRows.forEach(
              failedMutation -> {
                FailedElement failedElement =
                    toFailedElement(failedMutation, e, context.getPipelineOptions().getJobName());
                context.output(failedInsertsTupleTag, failedElement);
              });
          featureRows.clear();
        }
      }
    }
  }
}
