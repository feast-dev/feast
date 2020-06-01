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

import feast.proto.core.FeatureSetProto.EntitySpec;
import feast.proto.core.FeatureSetProto.FeatureSetSpec;
import feast.proto.core.FeatureSetProto.FeatureSpec;
import feast.proto.core.StoreProto.Store.BigtableConfig;
import feast.proto.storage.BigtableProto.BigtableKey;
import feast.proto.storage.BigtableProto.BigtableKey.Builder;
import feast.proto.types.FeatureRowProto.FeatureRow;
import feast.proto.types.FieldProto.Field;
import feast.proto.types.ValueProto;
import feast.storage.api.writer.FailedElement;
import feast.storage.api.writer.WriteResult;
import feast.storage.common.retry.Retriable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BigtableCustomIO {

  private static final int DEFAULT_BATCH_SIZE = 1000;
  private static final int DEFAULT_TIMEOUT = 2000;

  private static TupleTag<FeatureRow> successfulInsertsTag =
      new TupleTag<FeatureRow>("successfulInserts") {};
  private static TupleTag<FailedElement> failedInsertsTupleTag =
      new TupleTag<FailedElement>("failedInserts") {};

  private static final Logger log = LoggerFactory.getLogger(BigtableCustomIO.class);

  private BigtableCustomIO() {}

  public static Write write(BigtableConfig bigtableConfig, Map<String, FeatureSetSpec> featureSetSpecs) {
    return new Write(bigtableConfig, featureSetSpecs);
  }

  /** ServingStoreWrite data to a Bigtable server. */
  public static class Write extends PTransform<PCollection<FeatureRow>, WriteResult> {

    private Map<String, FeatureSetSpec> featureSetSpecs;
    private BigtableConfig bigtableConfig;
    private int batchSize;
    private int timeout;

    public Write(BigtableConfig bigtableConfig, Map<String, FeatureSetSpec> featureSetSpecs) {

      this.bigtableConfig = bigtableConfig;
      this.featureSetSpecs = featureSetSpecs;
    }

    public Write withBatchSize(int batchSize) {
      this.batchSize = batchSize;
      return this;
    }

    public Write withTimeout(int timeout) {
      this.timeout = timeout;
      return this;
    }

    @Override
    public WriteResult expand(PCollection<FeatureRow> input) {
      PCollectionTuple bigtableWrite =
          input.apply(
              ParDo.of(new WriteDoFn(bigtableConfig, featureSetSpecs))
                  .withOutputTags(successfulInsertsTag, TupleTagList.of(failedInsertsTupleTag)));
      return WriteResult.in(
          input.getPipeline(),
          bigtableWrite.get(successfulInsertsTag),
          bigtableWrite.get(failedInsertsTupleTag));
    }

    public static class WriteDoFn extends DoFn<FeatureRow, FeatureRow> {

      private final List<FeatureRow> featureRows = new ArrayList<>();
      private Map<String, FeatureSetSpec> featureSetSpecs;
      private int batchSize = DEFAULT_BATCH_SIZE;
      private int timeout = DEFAULT_TIMEOUT;
      private BigtableIngestionClient bigtableIngestionClient;

      WriteDoFn(BigtableConfig config, Map<String, FeatureSetSpec> featureSetSpecs) {

        this.bigtableIngestionClient = new BigtableStandaloneIngestionClient(config);
        this.featureSetSpecs = featureSetSpecs;
      }

      public WriteDoFn withBatchSize(int batchSize) {
        if (batchSize > 0) {
          this.batchSize = batchSize;
        }
        return this;
      }

      public WriteDoFn withTimeout(int timeout) {
        if (timeout > 0) {
          this.timeout = timeout;
        }
        return this;
      }

      @Setup
      public void setup() {
        this.bigtableIngestionClient.setup();
      }

      @StartBundle
      public void startBundle() {
        try {
          bigtableIngestionClient.connect();
        } catch (BigtableConnectionException e) {
          log.error("Connection to bigtable cannot be established ", e);
        }
        featureRows.clear();
      }

      private void executeBatch() throws Exception {
        this.bigtableIngestionClient
            .getBackOffExecutor()
            .execute(
                new Retriable() {
                  @Override
                  public void execute() throws ExecutionException, InterruptedException {
                    if (!bigtableIngestionClient.isConnected()) {
                      bigtableIngestionClient.connect();
                    }
                    featureRows.forEach(
                        row -> {
                          bigtableIngestionClient.set(getKey(row), getValue(row));
                        });
                    bigtableIngestionClient.sync();
                  }

                  @Override
                  public Boolean isExceptionRetriable(Exception e) {
                    return e instanceof BigtableConnectionException;
                  }

                  @Override
                  public void cleanUpAfterFailure() {}
                });
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

      private byte[] getKey(FeatureRow featureRow) {
        FeatureSetSpec featureSetSpec = featureSetSpecs.get(featureRow.getFeatureSet());
        List<String> entityNames =
            featureSetSpec.getEntitiesList().stream()
                .map(EntitySpec::getName)
                .sorted()
                .collect(Collectors.toList());

        Map<String, Field> entityFields = new HashMap<>();
        Builder bigtableKeyBuilder = BigtableKey.newBuilder().setFeatureSet(featureRow.getFeatureSet());
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
        return bigtableKeyBuilder.build().toByteArray();
      }

      private byte[] getValue(FeatureRow featureRow) {
        FeatureSetSpec spec = featureSetSpecs.get(featureRow.getFeatureSet());

        List<String> featureNames =
            spec.getFeaturesList().stream().map(FeatureSpec::getName).collect(Collectors.toList());
        Map<String, Field> fieldValueOnlyMap =
            featureRow.getFieldsList().stream()
                .filter(field -> featureNames.contains(field.getName()))
                .distinct()
                .collect(
                    Collectors.toMap(
                        Field::getName,
                        field -> Field.newBuilder().setValue(field.getValue()).build()));

        List<Field> values =
            featureNames.stream()
                .sorted()
                .map(
                    featureName ->
                        fieldValueOnlyMap.getOrDefault(
                            featureName,
                            Field.newBuilder()
                                .setValue(ValueProto.Value.getDefaultInstance())
                                .build()))
                .collect(Collectors.toList());

        return FeatureRow.newBuilder()
            .setEventTimestamp(featureRow.getEventTimestamp())
            .addAllFields(values)
            .build()
            .toByteArray();
      }

      @ProcessElement
      public void processElement(ProcessContext context) {
        FeatureRow featureRow = context.element();
        featureRows.add(featureRow);
        if (featureRows.size() >= batchSize) {
          try {
            executeBatch();
            featureRows.forEach(row -> context.output(successfulInsertsTag, row));
            featureRows.clear();
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

      @FinishBundle
      public void finishBundle(FinishBundleContext context)
          throws IOException, InterruptedException {
        if (featureRows.size() > 0) {
          try {
            executeBatch();
            featureRows.forEach(
                row ->
                    context.output(
                        successfulInsertsTag, row, Instant.now(), GlobalWindow.INSTANCE));
            featureRows.clear();
          } catch (Exception e) {
            featureRows.forEach(
                failedMutation -> {
                  FailedElement failedElement =
                      toFailedElement(failedMutation, e, context.getPipelineOptions().getJobName());
                  context.output(
                      failedInsertsTupleTag, failedElement, Instant.now(), GlobalWindow.INSTANCE);
                });
            featureRows.clear();
          }
        }
      }

      @Teardown
      public void teardown() {
        bigtableIngestionClient.shutdown();
      }
    }
  }
}
