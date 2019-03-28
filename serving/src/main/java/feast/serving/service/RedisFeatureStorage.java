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
import feast.serving.exception.FeatureRetrievalException;
import feast.serving.model.FeatureValue;
import feast.specs.FeatureSpecProto.FeatureSpec;
import feast.storage.RedisProto.RedisBucketKey;
import feast.storage.RedisProto.RedisBucketValue;
import io.opentracing.Scope;
import io.opentracing.Span;
import io.opentracing.Tracer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.codec.digest.DigestUtils;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

/** Class for retrieving features from a Redis instance. */
@Slf4j
public class RedisFeatureStorage implements FeatureStorage {

  public static final String TYPE = "redis";
  // BUCKET_ID_ZERO is used to identify feature with granularity NONE or latest value of a
  // time-series feature.
  private static final long BUCKET_ID_ZERO = 0;
  private static final byte[][] ZERO_LENGTH_ARRAY = new byte[0][0];
  public static String OPT_REDIS_HOST = "host";
  public static String OPT_REDIS_PORT = "port";
  public static String OPT_REDIS_BUCKET_SIZE = "bucketSize"; // ISO8601 Period
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

  /** {@inheritDoc} */
  @Override
  public List<FeatureValue> getFeature(
      String entityName, Collection<String> entityIds, Collection<FeatureSpec> featureSpecs) {
    try (Scope scope = tracer.buildSpan("Redis-getFeature").startActive(true)) {
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
   * Send a list of get request as an mget and process its result.
   *
   * @param getRequests list of get request.
   * @return list of feature value.
   */
  private List<FeatureValue> sendAndProcessMultiGet(List<GetRequest> getRequests) {
    try (Scope scope = tracer.buildSpan("Redis-sendAndProcessMultiGet").startActive(true)) {
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
    try (Scope scope = tracer.buildSpan("Redis-processMGet").startActive(true)) {

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
  private static class GetRequest {

    /** Entity Id of the get request */
    private String entityId;

    /** Feature Id of the get request */
    private String featureId;

    /** Bucket key of the request */
    private RedisBucketKey key;
  }
}
