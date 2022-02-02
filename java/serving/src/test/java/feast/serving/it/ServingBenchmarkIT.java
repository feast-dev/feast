/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright 2018-2021 The Feast Authors
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
package feast.serving.it;

import com.google.api.client.util.Lists;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableMap;
import com.google.common.math.Quantiles;
import feast.proto.serving.ServingAPIProto;
import feast.proto.types.ValueProto;
import feast.serving.config.ApplicationProperties;
import feast.serving.util.DataGenerator;
import java.util.List;
import java.util.LongSummaryStatistics;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ServingBenchmarkIT extends ServingEnvironment {
  private Random rand = new Random();
  public static final Logger log = LoggerFactory.getLogger(ServingBenchmarkIT.class);

  private static int WARM_UP_COUNT = 10;

  @Override
  ApplicationProperties.FeastProperties createFeastProperties() {
    return TestUtils.createBasicFeastProperties(
        environment.getServiceHost("redis", 6379), environment.getServicePort("redis", 6379));
  }

  protected ServingAPIProto.GetOnlineFeaturesRequest buildOnlineRequest(
      int rowsCount, int featuresCount) {
    List<ValueProto.Value> entities =
        IntStream.range(0, rowsCount)
            .mapToObj(
                i -> DataGenerator.createStrValue(String.format("key-%s", rand.nextInt(1000))))
            .collect(Collectors.toList());

    List<String> featureReferences =
        IntStream.range(0, featuresCount)
            .mapToObj(i -> String.format("feature_view_%d:feature_%d", i / 10, i))
            .collect(Collectors.toList());

    Map<String, ValueProto.RepeatedValue> entityRows =
        ImmutableMap.of(
            "entity", ValueProto.RepeatedValue.newBuilder().addAllVal(entities).build());

    return TestUtils.createOnlineFeatureRequest(featureReferences, entityRows);
  }

  protected ServingAPIProto.GetOnlineFeaturesRequest buildOnlineRequest(int rowsCount) {
    List<ValueProto.Value> entities =
        IntStream.range(0, rowsCount)
            .mapToObj(i -> DataGenerator.createInt64Value(rand.nextInt(1000)))
            .collect(Collectors.toList());

    Map<String, ValueProto.RepeatedValue> entityRows =
        ImmutableMap.of(
            "entity", ValueProto.RepeatedValue.newBuilder().addAllVal(entities).build());

    return TestUtils.createOnlineFeatureRequest("benchmark_feature_service", entityRows);
  }

  @Test
  public void benchmarkServing100rows10features() {
    ServingAPIProto.GetOnlineFeaturesRequest req = buildOnlineRequest(100, 10);

    measure(
        () -> servingStub.withDeadlineAfter(1, TimeUnit.SECONDS).getOnlineFeatures(req),
        "100 rows; 10 features",
        1000);
  }

  @Test
  public void benchmarkServing100rows50features() {
    ServingAPIProto.GetOnlineFeaturesRequest req = buildOnlineRequest(100, 50);

    measure(
        () -> servingStub.withDeadlineAfter(1, TimeUnit.SECONDS).getOnlineFeatures(req),
        "100 rows; 50 features",
        1000);
  }

  @Test
  public void benchmarkServing100rows100features() {
    ServingAPIProto.GetOnlineFeaturesRequest req = buildOnlineRequest(100, 100);

    measure(
        () -> servingStub.withDeadlineAfter(1, TimeUnit.SECONDS).getOnlineFeatures(req),
        "100 rows; 100 features",
        1000);
  }

  @Test
  public void benchmarkServing100rowsFullFeatureService() {
    ServingAPIProto.GetOnlineFeaturesRequest req = buildOnlineRequest(100);

    measure(
        () -> servingStub.withDeadlineAfter(1, TimeUnit.SECONDS).getOnlineFeatures(req),
        "100 rows; Full FS",
        1000);
  }

  private void measure(Runnable target, String name, int runs) {
    Stopwatch timer = Stopwatch.createUnstarted();

    List<Long> records = Lists.newArrayList();

    for (int i = 0; i < runs; i++) {
      timer.reset();
      timer.start();
      target.run();
      timer.stop();
      if (i >= WARM_UP_COUNT) {
        records.add(timer.elapsed(TimeUnit.MILLISECONDS));
      }
    }

    LongSummaryStatistics summary =
        records.stream().collect(Collectors.summarizingLong(Long::longValue));

    log.info(String.format("Test %s took (min): %d ms", name, summary.getMin()));
    log.info(String.format("Test %s took (avg): %f ms", name, summary.getAverage()));
    log.info(
        String.format("Test %s took (median): %f ms", name, Quantiles.median().compute(records)));
    log.info(
        String.format(
            "Test %s took (95p): %f ms", name, Quantiles.percentiles().index(95).compute(records)));
    log.info(
        String.format(
            "Test %s took (99p): %f ms", name, Quantiles.percentiles().index(99).compute(records)));
  }
}
