/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright 2018-2022 The Feast Authors
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
package feast.serving.util;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Singleton
public class RedisPerformanceMonitor {
  private static final Logger log = LoggerFactory.getLogger(RedisPerformanceMonitor.class);

  private final MeterRegistry meterRegistry;
  private final Timer redisOperationTimer;
  private final Counter redisErrorCounter;

  // For adaptive thresholding, keep track of recent latencies for HMGET and HGETALL
  // These are per-operation latencies, not per-key/per-batch.
  private static final int LATENCY_WINDOW_SIZE = 100; // Keep last 100 samples
  private final ConcurrentLinkedDeque<Long> hmgetLatenciesNanos;
  private final ConcurrentLinkedDeque<Long> hgetallLatenciesNanos;

  @Inject
  public RedisPerformanceMonitor(MeterRegistry meterRegistry) {
    this.meterRegistry = meterRegistry;
    this.redisOperationTimer =
        Timer.builder("feast_serving_redis_operation_duration_seconds")
            .description("Duration of Redis operations")
            .tag("component", "redis_retriever")
            .publishPercentiles(0.5, 0.95, 0.99)
            .register(meterRegistry);

    this.redisErrorCounter =
        Counter.builder("feast_serving_redis_operation_errors_total")
            .description("Total number of Redis operation errors")
            .tag("component", "redis_retriever")
            .register(meterRegistry);

    this.hmgetLatenciesNanos = new ConcurrentLinkedDeque<>();
    this.hgetallLatenciesNanos = new ConcurrentLinkedDeque<>();

    log.info("RedisPerformanceMonitor initialized.");
  }

  public void recordRedisOperation(
      String operationType, int keyCount, int fieldCount, long durationNanos) {
    redisOperationTimer
        .withTags(
            "operation",
            operationType,
            "key_count_bucket",
            bucketize(keyCount),
            "field_count_bucket",
            bucketize(fieldCount))
        .record(durationNanos, TimeUnit.NANOSECONDS);
  }

  public void recordRedisError(String operationType) {
    redisErrorCounter.withTag("operation", operationType).increment();
  }

  public void recordHmgetLatency(long durationNanos) {
    addLatencySample(hmgetLatenciesNanos, durationNanos);
  }

  public void recordHgetallLatency(long durationNanos) {
    addLatencySample(hgetallLatenciesNanos, durationNanos);
  }

  private void addLatencySample(ConcurrentLinkedDeque<Long> deque, long latencyNanos) {
    deque.addLast(latencyNanos);
    while (deque.size() > LATENCY_WINDOW_SIZE) {
      deque.pollFirst();
    }
  }

  public double getAverageHmgetLatency() {
    return calculateAverageLatency(hmgetLatenciesNanos);
  }

  public double getAverageHgetallLatency() {
    return calculateAverageLatency(hgetallLatenciesNanos);
  }

  private double calculateAverageLatency(ConcurrentLinkedDeque<Long> deque) {
    if (deque.isEmpty()) {
      return 0.0;
    }
    // Create a snapshot to iterate over for consistency
    Long[] latencies = deque.toArray(new Long[0]);
    if (latencies.length == 0) {
      return 0.0;
    }
    double sum = 0;
    for (Long latency : latencies) {
      if (latency != null) {
        sum += latency;
      }
    }
    return sum / latencies.length;
  }

  private String bucketize(int count) {
    if (count <= 0) return "0";
    if (count == 1) return "1";
    if (count <= 5) return "2-5";
    if (count <= 10) return "6-10";
    if (count <= 20) return "11-20";
    if (count <= 50) return "21-50";
    if (count <= 100) return "51-100";
    return "100+";
  }

  // Example of how to use the timer if you want to wrap an operation
  public <T> T measureOperation(
      String operationType, int keyCount, int fieldCount, CallableThrowsException<T> callable)
      throws Exception {
    long startTime = System.nanoTime();
    try {
      T result = callable.call();
      recordRedisOperation(operationType, keyCount, fieldCount, System.nanoTime() - startTime);
      return result;
    } catch (Exception e) {
      recordRedisError(operationType);
      recordRedisOperation(
          operationType + "_error", keyCount, fieldCount, System.nanoTime() - startTime);
      throw e;
    }
  }

  @FunctionalInterface
  public interface CallableThrowsException<V> {
    V call() throws Exception;
  }
}
