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
package feast.store.serving.redis;

import feast.core.StoreProto;
import feast.ingestion.values.FailedElement;
import feast.retry.Retriable;
import io.lettuce.core.RedisConnectionException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;
import org.apache.avro.reflect.Nullable;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.values.PCollection;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RedisCustomIO {

  private static final int DEFAULT_BATCH_SIZE = 1000;
  private static final int DEFAULT_TIMEOUT = 2000;

  private static final Logger log = LoggerFactory.getLogger(RedisCustomIO.class);

  private RedisCustomIO() {}

  public static Write write(StoreProto.Store store) {
    return new Write(store);
  }

  public enum Method {

    /**
     * Use APPEND command. If key already exists and is a string, this command appends the value at
     * the end of the string.
     */
    APPEND,

    /** Use SET command. If key already holds a value, it is overwritten. */
    SET,

    /**
     * Use LPUSH command. Insert value at the head of the list stored at key. If key does not exist,
     * it is created as empty list before performing the push operations. When key holds a value
     * that is not a list, an error is returned.
     */
    LPUSH,

    /**
     * Use RPUSH command. Insert value at the tail of the list stored at key. If key does not exist,
     * it is created as empty list before performing the push operations. When key holds a value
     * that is not a list, an error is returned.
     */
    RPUSH,

    /**
     * Use SADD command. Insert value into a set with a defined key. If key does not exist, it is
     * created as empty set before performing the add operations. When key holds a value that is not
     * a set, an error is returned.
     */
    SADD,

    /**
     * Use ZADD command. Adds all the specified members with the specified scores to the sorted set
     * stored at key. It is possible to specify multiple score / member pairs. If a specified member
     * is already a member of the sorted set, the score is updated and the element reinserted at the
     * right position to ensure the correct ordering.
     */
    ZADD
  }

  @DefaultCoder(AvroCoder.class)
  public static class RedisMutation {

    private Method method;
    private byte[] key;
    private byte[] value;
    @Nullable private Long expiryMillis;
    @Nullable private Long score;

    public RedisMutation() {}

    public RedisMutation(
        Method method,
        byte[] key,
        byte[] value,
        @Nullable Long expiryMillis,
        @Nullable Long score) {
      this.method = method;
      this.key = key;
      this.value = value;
      this.expiryMillis = expiryMillis;
      this.score = score;
    }

    public Method getMethod() {
      return method;
    }

    public void setMethod(Method method) {
      this.method = method;
    }

    public byte[] getKey() {
      return key;
    }

    public void setKey(byte[] key) {
      this.key = key;
    }

    public byte[] getValue() {
      return value;
    }

    public void setValue(byte[] value) {
      this.value = value;
    }

    @Nullable
    public Long getExpiryMillis() {
      return expiryMillis;
    }

    public void setExpiryMillis(@Nullable Long expiryMillis) {
      this.expiryMillis = expiryMillis;
    }

    @Nullable
    public Long getScore() {
      return score;
    }

    public void setScore(@Nullable Long score) {
      this.score = score;
    }
  }

  /** ServingStoreWrite data to a Redis server. */
  public static class Write
      extends PTransform<PCollection<RedisMutation>, PCollection<FailedElement>> {

    private WriteDoFn dofn;

    private Write(StoreProto.Store store) {
      this.dofn = new WriteDoFn(store);
    }

    public Write withBatchSize(int batchSize) {
      this.dofn.withBatchSize(batchSize);
      return this;
    }

    public Write withTimeout(int timeout) {
      this.dofn.withTimeout(timeout);
      return this;
    }

    @Override
    public PCollection<FailedElement> expand(PCollection<RedisMutation> input) {
      return input.apply(ParDo.of(dofn));
    }

    public static class WriteDoFn extends DoFn<RedisMutation, FailedElement> {

      private final List<RedisMutation> mutations = new ArrayList<>();
      private int batchSize = DEFAULT_BATCH_SIZE;
      private int timeout = DEFAULT_TIMEOUT;
      private RedisIngestionClient redisIngestionClient;

      WriteDoFn(StoreProto.Store store) {
        if (store.getType() == StoreProto.Store.StoreType.REDIS)
          this.redisIngestionClient = new RedisStandaloneIngestionClient(store.getRedisConfig());
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
        this.redisIngestionClient.setup();
      }

      @StartBundle
      public void startBundle() {
        try {
          redisIngestionClient.connect();
        } catch (RedisConnectionException e) {
          log.error("Connection to redis cannot be established ", e);
        }
        mutations.clear();
      }

      private void executeBatch() throws Exception {
        this.redisIngestionClient
            .getBackOffExecutor()
            .execute(
                new Retriable() {
                  @Override
                  public void execute() throws ExecutionException, InterruptedException {
                    if (!redisIngestionClient.isConnected()) {
                      redisIngestionClient.connect();
                    }
                    mutations.forEach(
                        mutation -> {
                          writeRecord(mutation);
                          if (mutation.getExpiryMillis() != null
                              && mutation.getExpiryMillis() > 0) {
                            redisIngestionClient.pexpire(
                                mutation.getKey(), mutation.getExpiryMillis());
                          }
                        });
                    redisIngestionClient.sync();
                    mutations.clear();
                  }

                  @Override
                  public Boolean isExceptionRetriable(Exception e) {
                    return e instanceof RedisConnectionException;
                  }

                  @Override
                  public void cleanUpAfterFailure() {}
                });
      }

      private FailedElement toFailedElement(
          RedisMutation mutation, Exception exception, String jobName) {
        return FailedElement.newBuilder()
            .setJobName(jobName)
            .setTransformName("RedisCustomIO")
            .setPayload(Arrays.toString(mutation.getValue()))
            .setErrorMessage(exception.getMessage())
            .setStackTrace(ExceptionUtils.getStackTrace(exception))
            .build();
      }

      @ProcessElement
      public void processElement(ProcessContext context) {
        RedisMutation mutation = context.element();
        mutations.add(mutation);
        if (mutations.size() >= batchSize) {
          try {
            executeBatch();
          } catch (Exception e) {
            mutations.forEach(
                failedMutation -> {
                  FailedElement failedElement =
                      toFailedElement(failedMutation, e, context.getPipelineOptions().getJobName());
                  context.output(failedElement);
                });
            mutations.clear();
          }
        }
      }

      private void writeRecord(RedisMutation mutation) {
        switch (mutation.getMethod()) {
          case APPEND:
            redisIngestionClient.append(mutation.getKey(), mutation.getValue());
            return;
          case SET:
            redisIngestionClient.set(mutation.getKey(), mutation.getValue());
            return;
          case LPUSH:
            redisIngestionClient.lpush(mutation.getKey(), mutation.getValue());
            return;
          case RPUSH:
            redisIngestionClient.rpush(mutation.getKey(), mutation.getValue());
            return;
          case SADD:
            redisIngestionClient.sadd(mutation.getKey(), mutation.getValue());
            return;
          case ZADD:
            redisIngestionClient.zadd(mutation.getKey(), mutation.getScore(), mutation.getValue());
            return;
          default:
            throw new UnsupportedOperationException(
                String.format("Not implemented writing records for %s", mutation.getMethod()));
        }
      }

      @FinishBundle
      public void finishBundle(FinishBundleContext context)
          throws IOException, InterruptedException {
        if (mutations.size() > 0) {
          try {
            executeBatch();
          } catch (Exception e) {
            mutations.forEach(
                failedMutation -> {
                  FailedElement failedElement =
                      toFailedElement(failedMutation, e, context.getPipelineOptions().getJobName());
                  context.output(failedElement, Instant.now(), GlobalWindow.INSTANCE);
                });
            mutations.clear();
          }
        }
      }

      @Teardown
      public void teardown() {
        redisIngestionClient.shutdown();
      }
    }
  }
}
