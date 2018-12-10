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

import com.google.protobuf.InvalidProtocolBufferException;
import feast.serving.ServingAPIProto.TimestampRange;
import feast.serving.exception.FeatureRetrievalException;
import feast.serving.model.FeatureValue;
import feast.serving.model.Pair;
import feast.serving.util.SpecUtil;
import feast.serving.util.TimeUtil;
import feast.specs.FeatureSpecProto.FeatureSpec;
import feast.storage.RedisProto.RedisBucketKey;
import feast.storage.RedisProto.RedisBucketValue;
import io.opentracing.Scope;
import io.opentracing.Span;
import io.opentracing.Tracer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.codec.digest.DigestUtils;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;

/**
 * Class for retrieving features from a Redis instance.
 */
@Slf4j
public class RedisFeatureStorage implements FeatureStorage {

  public static final String TYPE = "redis";

  public static String OPT_REDIS_HOST = "host";
  public static String OPT_REDIS_PORT = "port";

  public static String OPT_REDIS_BUCKET_SIZE = "bucketSize"; // ISO8601 Period

  // BUCKET_ID_ZERO is used to identify feature with granularity NONE or latest value of a
  // time-series feature.
  private static final long BUCKET_ID_ZERO = 0;

  private static final byte[][] ZERO_LENGTH_ARRAY = new byte[0][0];

  private final JedisPool jedisPool;
  private final Tracer tracer;

  /**
   * Create a RedisFeatureStorage.
   *
   * @param jedisPool pool of Jedis instance configured to connect to certain Redis instance.
   */
  public RedisFeatureStorage(JedisPool jedisPool, Tracer tracer) {
    this.jedisPool = jedisPool;
    this.tracer = tracer;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public List<FeatureValue> getCurrentFeature(
      String entityName, List<String> entityIds, FeatureSpec featureSpec)
      throws FeatureRetrievalException {
    try (Scope scope = tracer.buildSpan("Redis-getCurrentFeature").startActive(true)) {
      String featureId = featureSpec.getId();
      String featureIdSha1Prefix = makeFeatureIdSha1Prefix(featureId);
      List<GetRequest> getRequests = new ArrayList<>(entityIds.size());
      for (String entityId : entityIds) {
        RedisBucketKey key = makeBucketKey(entityId, featureIdSha1Prefix, BUCKET_ID_ZERO);
        getRequests.add(new GetRequest(entityId, featureId, key));
      }
      scope.span().log("completed request creation");
      return sendAndProcessMultiGet(getRequests);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public List<FeatureValue> getCurrentFeatures(
      String entityName, List<String> entityIds, List<FeatureSpec> featureSpecs) {
    try (Scope scope = tracer.buildSpan("Redis-getCurrentFeatures").startActive(true)) {
      List<GetRequest> getRequests = new ArrayList<>(entityIds.size() * featureSpecs.size());
      for (FeatureSpec featureSpec : featureSpecs) {
        String featureId = featureSpec.getId();
        String featureIdSha1Prefix = makeFeatureIdSha1Prefix(featureId);
        for (String entityId : entityIds) {
          RedisBucketKey key = makeBucketKey(entityId, featureIdSha1Prefix, BUCKET_ID_ZERO);
          getRequests.add(new GetRequest(entityId, featureId, key));
        }
      }
      scope.span().log("completed request creation");
      return sendAndProcessMultiGet(getRequests);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public List<FeatureValue> getNLatestFeatureWithinTimestampRange(
      String entityName,
      List<String> entityIds,
      Pair<FeatureSpec, Integer> featureSpecLimitPair,
      TimestampRange tsRange)
      throws FeatureRetrievalException {
    try (Scope scope = tracer.buildSpan("Redis-getNLatestFeatureWithinTimestampRange")
        .startActive(true)) {
      List<PipelinedBucketKeys> buckets =
          makePipelinedBucketKeys(
              entityIds, featureSpecLimitPair.getLeft(), featureSpecLimitPair.getRight(), tsRange);
      sendAndSyncPipelinedBucket(buckets);

      List<GetRequest> getRequests = new ArrayList<>();
      Map<String, Integer> featureCount =
          new HashMap<>(entityIds.size()); // map of entity id to the number of value.
      for (PipelinedBucketKeys bucket : buckets) {
        for (byte[] rawKey : bucket.getBucket().get()) {
          Integer count = featureCount.computeIfAbsent(bucket.getEntityId(), k -> 0);
          if (count < bucket.getLimit()) {
            RedisBucketKey key;
            try {
              key = RedisBucketKey.parseFrom(rawKey);
            } catch (InvalidProtocolBufferException e) {
              log.warn("Unable to parse one of key: {}", rawKey);
              continue;
            }
            getRequests.add(new GetRequest(bucket.entityId, bucket.featureId, key));
            count++;
            featureCount.put(bucket.getEntityId(), count);
          } else {
            break;
          }
        }
      }

      return sendAndProcessMultiGet(getRequests);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public List<FeatureValue> getNLatestFeaturesWithinTimestampRange(
      String entityName,
      List<String> entityIds,
      List<Pair<FeatureSpec, Integer>> featureSpecAndLimitPairs,
      TimestampRange tsRange) {
    try (Scope scope = tracer.buildSpan("Redis-getNLatestFeaturesWithinTimestampRange")
        .startActive(true)) {

      List<PipelinedBucketKeys> buckets = new ArrayList<>();
      for (Pair<FeatureSpec, Integer> featureSpecAndLimitPair : featureSpecAndLimitPairs) {
        buckets.addAll(
            makePipelinedBucketKeys(
                entityIds,
                featureSpecAndLimitPair.getLeft(),
                featureSpecAndLimitPair.getRight(),
                tsRange));
      }
      sendAndSyncPipelinedBucket(buckets);

      List<GetRequest> getRequests = new ArrayList<>();
      Map<String, Map<String, Integer>> entityCountMap = new HashMap<>(entityIds.size());
      for (PipelinedBucketKeys bucket : buckets) {
        for (byte[] rawKey : bucket.getBucket().get()) {
          Map<String, Integer> featureCountMap =
              entityCountMap.computeIfAbsent(
                  bucket.getEntityId(), k -> new HashMap<>(featureSpecAndLimitPairs.size()));
          Integer count = featureCountMap.computeIfAbsent(bucket.getFeatureId(), k -> 0);

          if (count < bucket.getLimit()) {
            String entityId = bucket.getEntityId();
            String featureId = bucket.getFeatureId();
            RedisBucketKey key;
            try {
              key = RedisBucketKey.parseFrom(rawKey);
            } catch (InvalidProtocolBufferException e) {
              log.warn("Unable to parse one of key: {}", rawKey);
              continue;
            }

            getRequests.add(new GetRequest(entityId, featureId, key));
            count++;
            featureCountMap.put(bucket.getFeatureId(), count);
            entityCountMap.put(bucket.getEntityId(), featureCountMap);
          } else {
            break;
          }
        }
      }

      return sendAndProcessMultiGet(getRequests);
    }
  }

  /**
   * Send a list of get request as an mget and process its result.
   *
   * @param getRequests list of get request.
   * @return list of feature value.
   */
  private List<FeatureValue> sendAndProcessMultiGet(List<GetRequest> getRequests) {
    try (Scope scope = tracer.buildSpan("Redis-sendAndProcessMultiGet")
        .startActive(true)) {
      Span span = scope.span();

      if (getRequests.isEmpty()) {
        return Collections.emptyList();
      }

      span.log("creating mget request");
      byte[][] binaryKeys =
          getRequests
              .stream()
              .map(r -> r.getKey().toByteArray())
              .collect(Collectors.toList())
              .toArray(ZERO_LENGTH_ARRAY);
      span.log("completed creating mget request");

      List<byte[]> binaryValues;
      try (Jedis jedis = jedisPool.getResource()) {
        span.log("sending mget");
        binaryValues = jedis.mget(binaryKeys);
        span.log("completed mget");
      } catch (Exception e) {
        log.error("Exception while retrieving feature from Redis", e);
        throw new FeatureRetrievalException("Unable to retrieve feature from Redis", e);
      }

      try {
        return processMGet(getRequests, binaryValues);
      } catch (InvalidProtocolBufferException e) {
        log.error("Unable to parse protobuf", e);
        throw new FeatureRetrievalException("Unable to parse protobuf while retrieving feature", e);
      }
    }
  }

  /**
   * Process mget results given a list of get requests.
   *
   * @param requests list of get request.
   * @param results list of get result.
   * @return list of feature value.
   * @throws InvalidProtocolBufferException if protobuf parsing fail.
   */
  private List<FeatureValue> processMGet(List<GetRequest> requests, List<byte[]> results)
      throws InvalidProtocolBufferException {
    try (Scope scope = tracer.buildSpan("Redis-processMGet")
        .startActive(true)) {

      int keySize = requests.size();
      List<FeatureValue> featureValues = new ArrayList<>(keySize);
      for (int i = 0; i < keySize; i++) {
        GetRequest request = requests.get(i);
        byte[] binaryValue = results.get(i);
        if (binaryValue == null) {
          continue;
        }
        RedisBucketValue value = RedisBucketValue.parseFrom(binaryValue);
        FeatureValue featureValue =
            new FeatureValue(
                request.getFeatureId(),
                request.getEntityId(),
                value.getValue(),
                value.getEventTimestamp());
        featureValues.add(featureValue);
      }
      return featureValues;
    }
  }

  /**
   * Send and wait for result of a pipelined zrevrangeByScore.
   *
   * @param buckets list of keys to be scanned.
   */
  private void sendAndSyncPipelinedBucket(List<PipelinedBucketKeys> buckets) {
    try (Scope scope = tracer.buildSpan("Redis-sendAndSyncPipelinedBucket")
        .startActive(true);
        Jedis jedis = jedisPool.getResource()) {
      Pipeline pipeline = jedis.pipelined();

      // retrieve all redis keys from index bucket.
      for (PipelinedBucketKeys bucket : buckets) {
        bucket.setBucket(
            pipeline.zrevrangeByScore(
                bucket.getBucketKey().toByteArray(),
                bucket.getMaxTimestamp(),
                bucket.getMinTimestamp()));
      }

      pipeline.sync();
    } catch (Exception e) {
      log.error("Exception while retrieving bucket keys in redis", e);
      throw new FeatureRetrievalException("Unable to retrieve feature from Redis", e);
    }
  }

  /**
   * Make a list of bucket keys to scan.
   *
   * @param entityIds list of entity id.
   * @param featureSpec feature spec of the feature to be scanned.
   * @param limit maximum number of value returned from request.
   * @param tsRange timestamp range of value to be returned.
   * @return list of bucket keys to be scanned in pipelined command.
   */
  private List<PipelinedBucketKeys> makePipelinedBucketKeys(
      List<String> entityIds, FeatureSpec featureSpec, int limit, TimestampRange tsRange) {
    try (Scope scope = tracer.buildSpan("Redis-makePipelinedBucketKeys")
        .startActive(true)) {
      List<PipelinedBucketKeys> buckets = new ArrayList<>();
      String featureId = featureSpec.getId();
      String featureIdSha1Prefix = makeFeatureIdSha1Prefix(featureId);

      long maxTimestamp =
          TimeUtil.roundFloorTimestamp(tsRange.getEnd(), featureSpec.getGranularity()).getSeconds();
      long minTimestamp =
          TimeUtil.roundFloorTimestamp(tsRange.getStart(), featureSpec.getGranularity())
              .getSeconds();

      for (String entityId : entityIds) {
        for (RedisBucketKey bucketKey :
            makeBucketKeysToScan(
                entityId, featureIdSha1Prefix, minTimestamp, maxTimestamp, featureSpec)) {
          buckets.add(
              new PipelinedBucketKeys(
                  entityId, featureId, bucketKey, null, minTimestamp, maxTimestamp, limit));
        }
      }
      return buckets;
    }
  }

  /**
   * Generate list of bucket keys to be scanned for range query.
   *
   * @param entityId entity ID of the feature
   * @param featureIdSha1Prefix first 7 bytes of feature ID SHA1 prefix.
   * @param roundedStart start time of range query rounded into granularity.
   * @param roundedEnd end time of range query rounded into granualrity.
   * @param featureSpec feature spec of the feature.
   * @return list of redis bucket key to be scanned.
   */
  private List<RedisBucketKey> makeBucketKeysToScan(
      String entityId,
      String featureIdSha1Prefix,
      long roundedStart,
      long roundedEnd,
      FeatureSpec featureSpec) {
    List<RedisBucketKey> bucketKeys = new ArrayList<>();
    long bucketSizeInSecond = SpecUtil.getBucketSize(featureSpec).getStandardSeconds();

    for (long i = roundedStart; i <= roundedEnd; i += bucketSizeInSecond) {
      bucketKeys.add(
          RedisBucketKey.newBuilder()
              .setEntityKey(entityId)
              .setFeatureIdSha1Prefix(featureIdSha1Prefix)
              .setBucketId(i / bucketSizeInSecond)
              .build());
    }
    return bucketKeys;
  }

  /**
   * Create {@link RedisBucketKey}.
   *
   * @param entityId entityID of the feature
   * @param featureIdSha1Prefix first 7-bytes of featureId SHA1 prefix
   * @param bucketId bucket ID.
   * @return instance of RedisBucketKey
   */
  private RedisBucketKey makeBucketKey(String entityId, String featureIdSha1Prefix, long bucketId) {
    return RedisBucketKey.newBuilder()
        .setEntityKey(entityId)
        .setFeatureIdSha1Prefix(featureIdSha1Prefix)
        .setBucketId(bucketId)
        .build();
  }

  /**
   * Calculate feature id's sha1 prefix.
   *
   * @param featureId feature ID
   * @return first 7 characters of SHA1(featureID)
   */
  private String makeFeatureIdSha1Prefix(String featureId) {
    return DigestUtils.sha1Hex(featureId.getBytes()).substring(0, 7);
  }

  @AllArgsConstructor
  @Getter
  @Setter
  private static class PipelinedBucketKeys {

    /**
     * Entity Id of the feature
     */
    private String entityId;

    /**
     * Feature Id of the feature
     */
    private String featureId;

    /**
     * bucket key
     */
    private RedisBucketKey bucketKey;

    /**
     * Content of bucket
     */
    private Response<Set<byte[]>> bucket;

    /**
     * Rounded lower bound of timestamp to be scanned
     */
    private long minTimestamp;

    /**
     * Rounded upper bound of timestamp to be scanned
     */
    private long maxTimestamp;

    /**
     * Maximum number of value to be returned
     */
    private int limit;
  }

  @AllArgsConstructor
  @Getter
  private static class GetRequest {

    /**
     * Entity Id of the get request
     */
    private String entityId;

    /**
     * Feature Id of the get request
     */
    private String featureId;

    /**
     * Bucket key of the request
     */
    private RedisBucketKey key;
  }
}
