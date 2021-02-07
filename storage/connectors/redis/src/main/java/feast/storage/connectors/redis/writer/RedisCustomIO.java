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

import static feast.storage.connectors.redis.writer.RedisCustomIOUtil.deduplicateRows;
import static feast.storage.connectors.redis.writer.RedisCustomIOUtil.getKey;
import static feast.storage.connectors.redis.writer.RedisCustomIOUtil.getValue;
import static feast.storage.connectors.redis.writer.RedisCustomIOUtil.rowShouldBeWritten;

import com.google.common.collect.Iterators;
import feast.proto.core.FeatureSetProto.FeatureSetSpec;
import feast.proto.storage.RedisProto.RedisKey;
import feast.proto.types.FeatureRowProto.FeatureRow;
import feast.storage.api.writer.FailedElement;
import feast.storage.api.writer.WriteResult;
import feast.storage.connectors.redis.serializer.RedisKeySerializer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupIntoBatches;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RedisCustomIO {

  private static TupleTag<FeatureRow> successfulInsertsTag =
      new TupleTag<FeatureRow>("successfulInserts") {};
  private static TupleTag<FailedElement> failedInsertsTupleTag =
      new TupleTag<FailedElement>("failedInserts") {};

  private static final Logger log = LoggerFactory.getLogger(RedisCustomIO.class);

  private RedisCustomIO() {}

  public static Write write(
      RedisIngestionClient redisIngestionClient,
      PCollectionView<Map<String, Iterable<FeatureSetSpec>>> featureSetSpecs,
      RedisKeySerializer serializer) {
    return new Write(redisIngestionClient, featureSetSpecs, serializer);
  }

  /** ServingStoreWrite data to a Redis server. */
  public static class Write extends PTransform<PCollection<FeatureRow>, WriteResult> {

    private PCollectionView<Map<String, Iterable<FeatureSetSpec>>> featureSetSpecs;
    private RedisIngestionClient redisIngestionClient;
    private RedisKeySerializer serializer;
    private int batchSize;
    private Duration flushFrequency;

    public Write(
        RedisIngestionClient redisIngestionClient,
        PCollectionView<Map<String, Iterable<FeatureSetSpec>>> featureSetSpecs,
        RedisKeySerializer serializer) {
      this.redisIngestionClient = redisIngestionClient;
      this.featureSetSpecs = featureSetSpecs;
      this.serializer = serializer;
    }

    public Write withBatchSize(int batchSize) {
      this.batchSize = batchSize;
      return this;
    }

    public Write withFlushFrequency(Duration frequency) {
      this.flushFrequency = frequency;
      return this;
    }

    @Override
    public WriteResult expand(PCollection<FeatureRow> input) {
      PCollectionTuple redisWrite =
          input
              .apply("FixedFlushWindow", Window.<FeatureRow>into(FixedWindows.of(flushFrequency)))
              .apply(
                  "AttachFeatureReferenceKey",
                  ParDo.of(
                      new DoFn<FeatureRow, KV<String, FeatureRow>>() {
                        @ProcessElement
                        public void process(ProcessContext c) {
                          c.output(KV.of(c.element().getFeatureSet(), c.element()));
                        }
                      }))
              .apply("IntoBatches", GroupIntoBatches.ofSize(batchSize))
              .apply("ExtractResultValues", Values.create())
              .apply("GlobalWindow", Window.<Iterable<FeatureRow>>into(new GlobalWindows()))
              .apply(
                  ParDo.of(new WriteDoFn(redisIngestionClient, featureSetSpecs, serializer))
                      .withOutputTags(successfulInsertsTag, TupleTagList.of(failedInsertsTupleTag))
                      .withSideInputs(featureSetSpecs));
      return WriteResult.in(
          input.getPipeline(),
          redisWrite.get(successfulInsertsTag),
          redisWrite.get(failedInsertsTupleTag));
    }

    /**
     * Writes batch of {@link FeatureRow} to Redis. Only latest values should be written. In order
     * to guarantee that we first fetch all existing values (first batch operation), compare with
     * current batch by eventTimestamp, and send to redis values (second batch operation) that were
     * confirmed to be most recent.
     */
    public static class WriteDoFn extends BatchDoFnWithRedis<Iterable<FeatureRow>, FeatureRow> {

      private final PCollectionView<Map<String, Iterable<FeatureSetSpec>>> featureSetSpecsView;
      private final RedisKeySerializer serializer;

      WriteDoFn(
          RedisIngestionClient redisIngestionClient,
          PCollectionView<Map<String, Iterable<FeatureSetSpec>>> featureSetSpecsView,
          RedisKeySerializer serializer) {

        super(redisIngestionClient);
        this.featureSetSpecsView = featureSetSpecsView;
        this.serializer = serializer;
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

      @ProcessElement
      public void processElement(ProcessContext context) {
        List<FeatureRow> filteredFeatureRows = Collections.synchronizedList(new ArrayList<>());
        Map<String, FeatureSetSpec> latestSpecs =
            getLatestSpecs(context.sideInput(featureSetSpecsView));

        Map<RedisKey, FeatureRow> deduplicatedRows =
            deduplicateRows(context.element(), latestSpecs);

        try {
          executeBatch(
              (redisIngestionClient) ->
                  deduplicatedRows.entrySet().stream()
                      .map(
                          entry ->
                              redisIngestionClient
                                  .get(serializer.serialize(entry.getKey()))
                                  .thenAccept(
                                      currentValue -> {
                                        FeatureRow newRow = entry.getValue();
                                        if (rowShouldBeWritten(newRow, currentValue)) {
                                          filteredFeatureRows.add(newRow);
                                        }
                                      }))
                      .collect(Collectors.toList()));

          executeBatch(
              redisIngestionClient ->
                  filteredFeatureRows.stream()
                      .map(
                          row ->
                              redisIngestionClient.set(
                                  serializer.serialize(
                                      getKey(row, latestSpecs.get(row.getFeatureSet()))),
                                  getValue(row, latestSpecs.get(row.getFeatureSet()))
                                      .toByteArray()))
                      .collect(Collectors.toList()));

          filteredFeatureRows.forEach(row -> context.output(successfulInsertsTag, row));
        } catch (Exception e) {
          deduplicatedRows
              .values()
              .forEach(
                  failedMutation -> {
                    FailedElement failedElement =
                        toFailedElement(
                            failedMutation, e, context.getPipelineOptions().getJobName());
                    context.output(failedInsertsTupleTag, failedElement);
                  });
        }
      }

      Map<String, FeatureSetSpec> getLatestSpecs(Map<String, Iterable<FeatureSetSpec>> specs) {
        return specs.entrySet().stream()
            .map(e -> ImmutablePair.of(e.getKey(), Iterators.getLast(e.getValue().iterator())))
            .collect(Collectors.toMap(ImmutablePair::getLeft, ImmutablePair::getRight));
      }
    }
  }
}
