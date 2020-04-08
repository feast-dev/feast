/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright 2018-2020 The Feast Authors
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
package feast.core.service;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.*;

import feast.core.dao.EntityStatisticsRepository;
import feast.core.dao.FeatureStatisticsRepository;
import feast.core.dao.StoreRepository;
import java.util.Arrays;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.tensorflow.metadata.v0.*;
import org.tensorflow.metadata.v0.FeatureNameStatistics.Type;

public class StatsServiceTest {

  private StatsService statsService;
  @Mock private StoreRepository storeRepository;
  @Mock private FeatureStatisticsRepository featureStatisticsRepository;
  @Mock private EntityStatisticsRepository entityStatisticsRepository;
  @Mock private SpecService specService;

  @Before
  public void setUp() {
    statsService =
        new StatsService(
            storeRepository, specService, entityStatisticsRepository, featureStatisticsRepository);
  }

  @Test
  public void shouldAggregateNumericStatistics() {
    FeatureNameStatistics stat1 =
        FeatureNameStatistics.newBuilder()
            .setNumStats(
                NumericStatistics.newBuilder()
                    .setMax(20)
                    .setMin(1)
                    .setMean(6)
                    .setNumZeros(0)
                    .setStdDev(7.90569415)
                    .setCommonStats(
                        CommonStatistics.newBuilder()
                            .setTotNumValues(5)
                            .setNumNonMissing(5)
                            .setAvgNumValues(1)
                            .setMaxNumValues(1)
                            .setMinNumValues(1)
                            .setNumMissing(0)))
            .setType(Type.INT)
            .setPath(Path.newBuilder().addStep("feature").build())
            .build();
    FeatureNameStatistics stat2 =
        FeatureNameStatistics.newBuilder()
            .setNumStats(
                NumericStatistics.newBuilder()
                    .setMax(10)
                    .setMin(0)
                    .setMean(4)
                    .setNumZeros(1)
                    .setStdDev(3.807886553)
                    .setCommonStats(
                        CommonStatistics.newBuilder()
                            .setTotNumValues(5)
                            .setNumNonMissing(5)
                            .setAvgNumValues(1)
                            .setMaxNumValues(1)
                            .setMinNumValues(1)
                            .setNumMissing(1)))
            .setPath(Path.newBuilder().addStep("feature").build())
            .setType(Type.INT)
            .build();

    FeatureNameStatistics expected =
        FeatureNameStatistics.newBuilder()
            .setNumStats(
                NumericStatistics.newBuilder()
                    .setMax(20)
                    .setMin(0)
                    .setMean(5)
                    .setNumZeros(1)
                    .setStdDev(5.944184833146219)
                    .setCommonStats(
                        CommonStatistics.newBuilder()
                            .setTotNumValues(10)
                            .setNumNonMissing(10)
                            .setAvgNumValues(1)
                            .setMaxNumValues(1)
                            .setMinNumValues(1)
                            .setNumMissing(1)))
            .setPath(Path.newBuilder().addStep("feature").build())
            .setType(Type.INT)
            .build();

    assertThat(
        statsService.mergeStatistics(Arrays.asList(Arrays.asList(stat1, stat2))),
        equalTo(Arrays.asList(expected)));
  }

  @Test
  public void shouldAggregateCategoricalStatistics() {
    FeatureNameStatistics stat1 =
        FeatureNameStatistics.newBuilder()
            .setStringStats(
                StringStatistics.newBuilder()
                    .setCommonStats(
                        CommonStatistics.newBuilder()
                            .setTotNumValues(5)
                            .setNumNonMissing(5)
                            .setAvgNumValues(1)
                            .setMaxNumValues(1)
                            .setMinNumValues(1)
                            .setNumMissing(0))
                    .setUnique(4)
                    .setAvgLength(6))
            .setType(Type.STRING)
            .setPath(Path.newBuilder().addStep("feature").build())
            .build();
    FeatureNameStatistics stat2 =
        FeatureNameStatistics.newBuilder()
            .setStringStats(
                StringStatistics.newBuilder()
                    .setCommonStats(
                        CommonStatistics.newBuilder()
                            .setTotNumValues(5)
                            .setNumNonMissing(5)
                            .setAvgNumValues(1)
                            .setMaxNumValues(1)
                            .setMinNumValues(1)
                            .setNumMissing(1))
                    .setUnique(4)
                    .setAvgLength(4))
            .setType(Type.STRING)
            .setPath(Path.newBuilder().addStep("feature").build())
            .build();

    FeatureNameStatistics expected =
        FeatureNameStatistics.newBuilder()
            .setStringStats(
                StringStatistics.newBuilder()
                    .setCommonStats(
                        CommonStatistics.newBuilder()
                            .setTotNumValues(10)
                            .setNumNonMissing(10)
                            .setAvgNumValues(1)
                            .setMaxNumValues(1)
                            .setMinNumValues(1)
                            .setNumMissing(1))
                    .setAvgLength(5))
            .setType(Type.STRING)
            .setPath(Path.newBuilder().addStep("feature").build())
            .build();
    assertThat(
        statsService.mergeStatistics(Arrays.asList(Arrays.asList(stat1, stat2))),
        equalTo(Arrays.asList(expected)));
  }

  @Test
  public void shouldAggregateBytesStatistics() {
    FeatureNameStatistics stat1 =
        FeatureNameStatistics.newBuilder()
            .setBytesStats(
                BytesStatistics.newBuilder()
                    .setCommonStats(
                        CommonStatistics.newBuilder()
                            .setTotNumValues(5)
                            .setNumNonMissing(5)
                            .setAvgNumValues(1)
                            .setMaxNumValues(1)
                            .setMinNumValues(1)
                            .setNumMissing(0))
                    .setUnique(4)
                    .setAvgNumBytes(6)
                    .setMaxNumBytes(10)
                    .setMinNumBytes(0))
            .setType(Type.BYTES)
            .setPath(Path.newBuilder().addStep("feature").build())
            .build();
    FeatureNameStatistics stat2 =
        FeatureNameStatistics.newBuilder()
            .setBytesStats(
                BytesStatistics.newBuilder()
                    .setCommonStats(
                        CommonStatistics.newBuilder()
                            .setTotNumValues(5)
                            .setNumNonMissing(5)
                            .setAvgNumValues(1)
                            .setMaxNumValues(1)
                            .setMinNumValues(1)
                            .setNumMissing(1))
                    .setUnique(4)
                    .setAvgNumBytes(4)
                    .setMaxNumBytes(20)
                    .setMinNumBytes(1))
            .setType(Type.BYTES)
            .setPath(Path.newBuilder().addStep("feature").build())
            .build();

    FeatureNameStatistics expected =
        FeatureNameStatistics.newBuilder()
            .setBytesStats(
                BytesStatistics.newBuilder()
                    .setCommonStats(
                        CommonStatistics.newBuilder()
                            .setTotNumValues(10)
                            .setNumNonMissing(10)
                            .setAvgNumValues(1)
                            .setMaxNumValues(1)
                            .setMinNumValues(1)
                            .setNumMissing(1))
                    .setAvgNumBytes(5)
                    .setMaxNumBytes(20)
                    .setMinNumBytes(0))
            .setType(Type.BYTES)
            .setPath(Path.newBuilder().addStep("feature").build())
            .build();
    assertThat(
        statsService.mergeStatistics(Arrays.asList(Arrays.asList(stat1, stat2))),
        equalTo(Arrays.asList(expected)));
  }

  @Test
  public void shouldAggregateStructStatistics() {
    FeatureNameStatistics stat1 =
        FeatureNameStatistics.newBuilder()
            .setStructStats(
                StructStatistics.newBuilder()
                    .setCommonStats(
                        CommonStatistics.newBuilder()
                            .setTotNumValues(5)
                            .setNumNonMissing(5)
                            .setAvgNumValues(1)
                            .setMaxNumValues(1)
                            .setMinNumValues(1)
                            .setNumMissing(0)))
            .setType(Type.STRUCT)
            .setPath(Path.newBuilder().addStep("feature").build())
            .build();
    FeatureNameStatistics stat2 =
        FeatureNameStatistics.newBuilder()
            .setStructStats(
                StructStatistics.newBuilder()
                    .setCommonStats(
                        CommonStatistics.newBuilder()
                            .setTotNumValues(5)
                            .setNumNonMissing(5)
                            .setAvgNumValues(1)
                            .setMaxNumValues(1)
                            .setMinNumValues(1)
                            .setNumMissing(1)))
            .setType(Type.STRUCT)
            .setPath(Path.newBuilder().addStep("feature").build())
            .build();

    FeatureNameStatistics expected =
        FeatureNameStatistics.newBuilder()
            .setStructStats(
                StructStatistics.newBuilder()
                    .setCommonStats(
                        CommonStatistics.newBuilder()
                            .setTotNumValues(10)
                            .setNumNonMissing(10)
                            .setAvgNumValues(1)
                            .setMaxNumValues(1)
                            .setMinNumValues(1)
                            .setNumMissing(1)))
            .setType(Type.STRUCT)
            .setPath(Path.newBuilder().addStep("feature").build())
            .build();
    assertThat(
        statsService.mergeStatistics(Arrays.asList(Arrays.asList(stat1, stat2))),
        equalTo(Arrays.asList(expected)));
  }
}
