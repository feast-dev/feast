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
import com.google.common.collect.Streams;
import com.google.common.hash.Hashing;
import com.google.protobuf.InvalidProtocolBufferException;
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
import feast.storage.connectors.redis.retriever.FeatureRowDecoder;
import feast.storage.connectors.redis.serializer.RedisKeySerializer;
import java.nio.charset.StandardCharsets;
import java.time.ZonedDateTime;
import java.util.*;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.function.BinaryOperator;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.windowing.*;
import org.apache.beam.sdk.values.*;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Triple;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RedisCustomIO {

  private static final Random rand = new Random();

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

  // For unit testing the redis TTL jitter.
  static void setRandomSeed(long seed) {
    rand.setSeed(seed);
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

      // Used by unit tests to avoid race condition
      static Supplier<ZonedDateTime> currentTime = ZonedDateTime::now;

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

      private RedisKey getKey(FeatureRow featureRow, FeatureSetSpec spec) {
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
        return redisKeyBuilder.build();
      }

      private FailedElement processSkippedFeatureRow(FeatureRow featureRow, String jobName) {
        return FailedElement.newBuilder()
            .setJobName(jobName)
            .setTransformName("RedisCustomIO")
            .setPayload(featureRow.toString())
            .setErrorMessage("featurerow_to_redis_skipped")
            .build();
      }

      /**
       * Encode the Feature Row as bytes to store in Redis in encoded Feature Row encoding. To
       * reduce storage space consumption in redis, feature rows are "encoded" by hashing the fields
       * names and not unsetting the feature set reference. {@link FeatureRowDecoder} is
       * rensponsible for reversing this "encoding" step.
       */
      private FeatureRow getValue(FeatureRow featureRow, FeatureSetSpec spec) {
        List<String> featureNames =
            spec.getFeaturesList().stream().map(FeatureSpec::getName).collect(Collectors.toList());

        Map<String, Field.Builder> fieldValueOnlyMap =
            featureRow.getFieldsList().stream()
                .filter(field -> featureNames.contains(field.getName()))
                .distinct()
                .collect(
                    Collectors.toMap(
                        Field::getName, field -> Field.newBuilder().setValue(field.getValue())));

        List<Field> values =
            featureNames.stream()
                .sorted()
                .map(
                    featureName -> {
                      Field.Builder field =
                          fieldValueOnlyMap.getOrDefault(
                              featureName,
                              Field.newBuilder().setValue(ValueProto.Value.getDefaultInstance()));

                      // Encode the name of the as the hash of the field name.
                      // Use hash of name instead of the name of to reduce redis storage consumption
                      // per feature row stored.
                      String nameHash =
                          Hashing.murmur3_32()
                              .hashString(featureName, StandardCharsets.UTF_8)
                              .toString();
                      field.setName(nameHash);

                      return field.build();
                    })
                .collect(Collectors.toList());

        return FeatureRow.newBuilder()
            .setEventTimestamp(featureRow.getEventTimestamp())
            .addAllFields(values)
            .build();
      }

      @ProcessElement
      public void processElement(ProcessContext context) {
        boolean enableRedisTtl = getEnableRedisTtl();
        List<FeatureRow> filteredFeatureRows = Collections.synchronizedList(new ArrayList<>());
        List<FeatureRow> skippedFeatureRows = Collections.synchronizedList(new ArrayList<>());
        Map<String, FeatureSetSpec> latestSpecs =
            getLatestSpecs(context.sideInput(featureSetSpecsView));

        Map<RedisKey, FeatureRow> deduplicatedRows =
            deduplicateRows(context.element(), latestSpecs);

        List<Triple<RedisKey, FeatureRow, Optional<Long>>> rowsWithTtl =
            deduplicatedRows.entrySet().stream()
                .map(
                    entry -> {
                      FeatureRow row = entry.getValue();
                      FeatureSetSpec spec = latestSpecs.get(row.getFeatureSet());
                      Optional<Long> ttl =
                          enableRedisTtl
                              ? calculateRowTtl(
                                  row, spec, getMaxRedisTtlJitterSeconds(), getMaxRedisTtlSeconds())
                              : Optional.empty();
                      return Triple.of(entry.getKey(), row, ttl);
                    })
                .filter(
                    triplet -> {
                      if (triplet.getRight().isPresent() && triplet.getRight().get() <= 0) {
                        skippedFeatureRows.add(triplet.getMiddle());
                        return false;
                      }
                      return true;
                    })
                .collect(Collectors.toList());

        Map<RedisKey, Long> keysWithTtl =
            rowsWithTtl.stream()
                .filter(triplet -> triplet.getRight().isPresent())
                .collect(
                    Collectors.toMap(
                        triplet -> triplet.getLeft(), triplet -> triplet.getRight().get()));

        try {
          executeBatch(
              (redisIngestionClient) ->
                  rowsWithTtl.stream()
                      .map(
                          triplet ->
                              redisIngestionClient
                                  .get(serializer.serialize(triplet.getLeft()))
                                  .thenAccept(
                                      currentValue -> {
                                        FeatureRow newRow = triplet.getMiddle();
                                        if (rowShouldBeWritten(newRow, currentValue)) {
                                          filteredFeatureRows.add(newRow);
                                        }
                                      }))
                      .collect(Collectors.toList()));

          executeBatch(
              redisIngestionClient ->
                  filteredFeatureRows.stream()
                      .map(
                          row -> {
                            FeatureSetSpec spec = latestSpecs.get(row.getFeatureSet());
                            RedisKey key = getKey(row, spec);
                            byte[] value = getValue(row, spec).toByteArray();
                            if (keysWithTtl.containsKey(key)) {
                              long ttl = keysWithTtl.get(key);
                              return redisIngestionClient.setex(
                                  serializer.serialize(key), ttl, value);
                            } else {
                              return redisIngestionClient.set(serializer.serialize(key), value);
                            }
                          })
                      .collect(Collectors.toList()));

          filteredFeatureRows.forEach(row -> context.output(successfulInsertsTag, row));
          skippedFeatureRows.forEach(
              row -> processSkippedFeatureRow(row, context.getPipelineOptions().getJobName()));
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

      boolean rowShouldBeWritten(FeatureRow newRow, byte[] currentValue) {
        if (currentValue == null) {
          // nothing to compare with
          return true;
        }
        FeatureRow currentRow;
        try {
          currentRow = FeatureRow.parseFrom(currentValue);
        } catch (InvalidProtocolBufferException e) {
          // definitely need to replace current value
          return true;
        }

        // check whether new row has later eventTimestamp
        return new DateTime(currentRow.getEventTimestamp().getSeconds() * 1000L)
            .isBefore(new DateTime(newRow.getEventTimestamp().getSeconds() * 1000L));
      }

      /** Deduplicate rows by key within batch. Keep only latest eventTimestamp */
      Map<RedisKey, FeatureRow> deduplicateRows(
          Iterable<FeatureRow> rows, Map<String, FeatureSetSpec> latestSpecs) {
        Comparator<FeatureRow> byEventTimestamp =
            Comparator.comparing(r -> r.getEventTimestamp().getSeconds());

        FeatureRow identity =
            FeatureRow.newBuilder()
                .setEventTimestamp(
                    com.google.protobuf.Timestamp.newBuilder().setSeconds(-1).build())
                .build();

        return Streams.stream(rows)
            .collect(
                Collectors.groupingBy(
                    row -> getKey(row, latestSpecs.get(row.getFeatureSet())),
                    Collectors.reducing(identity, BinaryOperator.maxBy(byEventTimestamp))));
      }

      Map<String, FeatureSetSpec> getLatestSpecs(Map<String, Iterable<FeatureSetSpec>> specs) {
        return specs.entrySet().stream()
            .map(e -> ImmutablePair.of(e.getKey(), Iterators.getLast(e.getValue().iterator())))
            .collect(Collectors.toMap(ImmutablePair::getLeft, ImmutablePair::getRight));
      }

      /**
       * Helper function for calculating the TTL for the redis row.
       *
       * @return TTL in seconds for the redis record, or null if TTL should not be used.
       */
      Optional<Long> calculateRowTtl(
          FeatureRow newRow, FeatureSetSpec spec, int maxRedisTtlJitterSeconds, long maxRedisTtl) {
        long ttlSeconds = 0;
        final com.google.protobuf.Duration maxAge = spec.getMaxAge();

        // If maxAge isn't set don't apply TTL
        if (!maxAge.equals(com.google.protobuf.Duration.getDefaultInstance())) {
          ttlSeconds = maxAge.getSeconds();
          if (maxRedisTtl > 0 && ttlSeconds > maxRedisTtl) {
            ttlSeconds = maxRedisTtl;
          }

          // Adjust initial TTL based on event timestamp of
          // FeatureRow
          ttlSeconds -=
              currentTime.get().toInstant().getEpochSecond()
                  - newRow.getEventTimestamp().getSeconds();

          // The consideration to write data to Redis or not
          // should NOT take jitter into account
          if (ttlSeconds > 0 && maxRedisTtlJitterSeconds > 0) {
            ttlSeconds += rand.nextInt(maxRedisTtlJitterSeconds);
          }
        } else {
          if (maxRedisTtl > 0) {
            ttlSeconds = maxRedisTtl;
          } else {
            // Log an error instead of throwing an exception since setting a TTL is an
            // optimization for storage space usage.
            log.error("Unable to find FeatureSet to set Redis TTL  featureSet={}", spec.getName());
            return Optional.empty();
          }
        }

        return Optional.of(ttlSeconds);
      }
    }
  }
}
