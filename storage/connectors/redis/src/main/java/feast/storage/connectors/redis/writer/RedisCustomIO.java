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
package feast.storage.connectors.redis.writer;

import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import feast.proto.core.FeatureSetProto.EntitySpec;
import feast.proto.core.FeatureSetProto.FeatureSetSpec;
import feast.proto.core.FeatureSetProto.FeatureSpec;
import feast.proto.storage.RedisProto.RedisKey;
import feast.proto.storage.RedisProto.RedisKey.Builder;
import feast.proto.types.FeatureRowProto.FeatureRow;
import feast.proto.types.FieldProto.Field;
import feast.proto.types.ValueProto;
import feast.storage.api.writer.FailedElement;
import feast.storage.api.writer.WriteResult;
import feast.storage.common.retry.Retriable;
import io.lettuce.core.RedisException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.windowing.*;
import org.apache.beam.sdk.values.*;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RedisCustomIO {

  private static final int DEFAULT_BATCH_SIZE = 10000;
  private static final int DEFAULT_FREQUENCY_SECONDS = 30;

  private static TupleTag<FeatureRow> successfulInsertsTag =
      new TupleTag<FeatureRow>("successfulInserts") {};
  private static TupleTag<FailedElement> failedInsertsTupleTag =
      new TupleTag<FailedElement>("failedInserts") {};

  private static final Logger log = LoggerFactory.getLogger(RedisCustomIO.class);

  private RedisCustomIO() {}

  public static Write write(
      RedisIngestionClient redisIngestionClient,
      PCollectionView<Map<String, Iterable<FeatureSetSpec>>> featureSetSpecs) {
    return new Write(redisIngestionClient, featureSetSpecs);
  }

  /** ServingStoreWrite data to a Redis server. */
  public static class Write extends PTransform<PCollection<FeatureRow>, WriteResult> {

    private PCollectionView<Map<String, Iterable<FeatureSetSpec>>> featureSetSpecs;
    private RedisIngestionClient redisIngestionClient;
    private int batchSize = DEFAULT_BATCH_SIZE;
    private Duration flushFrequency = Duration.standardSeconds(DEFAULT_FREQUENCY_SECONDS);

    public Write(
        RedisIngestionClient redisIngestionClient,
        PCollectionView<Map<String, Iterable<FeatureSetSpec>>> featureSetSpecs) {
      this.redisIngestionClient = redisIngestionClient;
      this.featureSetSpecs = featureSetSpecs;
    }

    public Write withBatchSize(int batchSize) {
      this.batchSize = batchSize;
      return this;
    }

    public Write withFlushFrequency(Duration frequency) {
      this.flushFrequency = flushFrequency;
      return this;
    }

    @Override
    public WriteResult expand(PCollection<FeatureRow> input) {
      PCollectionTuple redisWrite =
          input
              .apply("FixedFlushWindow", Window.<FeatureRow>into(FixedWindows.of(flushFrequency)))
              .apply("AttachSingletonKey", WithKeys.of((Void) null))
              .apply("IntoBatches", GroupIntoBatches.ofSize(batchSize))
              .apply("ExtractResultValues", Values.create())
              .apply("GlobalWindow", Window.<Iterable<FeatureRow>>into(new GlobalWindows()))
              .apply(
                  ParDo.of(new WriteDoFn(redisIngestionClient, featureSetSpecs))
                      .withOutputTags(successfulInsertsTag, TupleTagList.of(failedInsertsTupleTag))
                      .withSideInputs(featureSetSpecs));
      return WriteResult.in(
          input.getPipeline(),
          redisWrite.get(successfulInsertsTag),
          redisWrite.get(failedInsertsTupleTag));
    }

    public static class WriteDoFn extends DoFn<Iterable<FeatureRow>, FeatureRow> {
      private PCollectionView<Map<String, Iterable<FeatureSetSpec>>> featureSetSpecsView;
      private RedisIngestionClient redisIngestionClient;

      WriteDoFn(
          RedisIngestionClient redisIngestionClient,
          PCollectionView<Map<String, Iterable<FeatureSetSpec>>> featureSetSpecsView) {

        this.redisIngestionClient = redisIngestionClient;
        this.featureSetSpecsView = featureSetSpecsView;
      }

      @Setup
      public void setup() {
        this.redisIngestionClient.setup();
      }

      @StartBundle
      public void startBundle() {
        try {
          redisIngestionClient.connect();
        } catch (RedisException e) {
          log.error("Connection to redis cannot be established ", e);
        }
      }

      private void executeBatch(
          Iterable<FeatureRow> featureRows, Map<String, FeatureSetSpec> featureSetSpecs)
          throws Exception {
        this.redisIngestionClient
            .getBackOffExecutor()
            .execute(
                new Retriable() {
                  @Override
                  public void execute() throws ExecutionException, InterruptedException {
                    if (!redisIngestionClient.isConnected()) {
                      redisIngestionClient.connect();
                    }
                    featureRows.forEach(
                        row -> {
                          redisIngestionClient.set(
                              getKey(row, featureSetSpecs.get(row.getFeatureSet())),
                              getValue(row, featureSetSpecs.get(row.getFeatureSet())));
                        });
                    redisIngestionClient.sync();
                  }

                  @Override
                  public Boolean isExceptionRetriable(Exception e) {
                    return e instanceof RedisException;
                  }

                  @Override
                  public void cleanUpAfterFailure() {}
                });
      }

      private FailedElement toFailedElement(
          FeatureRow featureRow, Exception exception, String jobName) {
        return FailedElement.newBuilder()
            .setJobName(jobName)
            .setTransformName("RedisCustomIO")
            .setPayload(featureRow.toString())
            .setErrorMessage(exception.getMessage())
            .setStackTrace(ExceptionUtils.getStackTrace(exception))
            .build();
      }

      private byte[] getKey(FeatureRow featureRow, FeatureSetSpec spec) {
        List<String> entityNames =
            spec.getEntitiesList().stream()
                .map(EntitySpec::getName)
                .sorted()
                .collect(Collectors.toList());

        Map<String, Field> entityFields = new HashMap<>();
        Builder redisKeyBuilder = RedisKey.newBuilder().setFeatureSet(featureRow.getFeatureSet());
        for (Field field : featureRow.getFieldsList()) {
          if (entityNames.contains(field.getName())) {
            entityFields.putIfAbsent(
                field.getName(),
                Field.newBuilder().setName(field.getName()).setValue(field.getValue()).build());
          }
        }
        for (String entityName : entityNames) {
          redisKeyBuilder.addEntities(entityFields.get(entityName));
        }
        return redisKeyBuilder.build().toByteArray();
      }

      private byte[] getValue(FeatureRow featureRow, FeatureSetSpec spec) {
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
        List<FeatureRow> featureRows = Lists.newArrayList(context.element().iterator());

        Map<String, FeatureSetSpec> latestSpecs =
            context.sideInput(featureSetSpecsView).entrySet().stream()
                .map(e -> ImmutablePair.of(e.getKey(), Iterators.getLast(e.getValue().iterator())))
                .collect(Collectors.toMap(ImmutablePair::getLeft, ImmutablePair::getRight));

        try {
          executeBatch(featureRows, latestSpecs);
          featureRows.forEach(row -> context.output(successfulInsertsTag, row));
        } catch (Exception e) {
          featureRows.forEach(
              failedMutation -> {
                FailedElement failedElement =
                    toFailedElement(failedMutation, e, context.getPipelineOptions().getJobName());
                context.output(failedInsertsTupleTag, failedElement);
              });
        }
      }

      @Teardown
      public void teardown() {
        redisIngestionClient.shutdown();
      }
    }
  }
}
