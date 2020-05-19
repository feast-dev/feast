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
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

import com.google.protobuf.Timestamp;
import feast.core.dao.FeatureSetRepository;
import feast.core.dao.FeatureStatisticsRepository;
import feast.core.dao.StoreRepository;
import feast.core.model.Project;
import feast.core.model.Store;
import feast.proto.core.CoreServiceProto.GetFeatureStatisticsRequest;
import feast.proto.core.StoreProto;
import feast.proto.core.StoreProto.Store.BigQueryConfig;
import feast.proto.core.StoreProto.Store.StoreType;
import java.io.IOException;
import java.util.Arrays;
import java.util.Optional;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mock;
import org.tensorflow.metadata.v0.*;
import org.tensorflow.metadata.v0.FeatureNameStatistics.Type;

public class StatsServiceTest {

  private StatsService statsService;
  @Mock private FeatureStatisticsRepository featureStatisticsRepository;
  @Mock private StoreRepository storeRepository;
  @Mock private FeatureSetRepository featureSetRepository;

  @Rule public final ExpectedException expectedException = ExpectedException.none();

  @Before
  public void setUp() {
    initMocks(this);
    statsService =
        new StatsService(storeRepository, featureStatisticsRepository, featureSetRepository);
  }

  @Test
  public void shouldThrowExceptionIfNeitherDatesNorDatasetsProvided() throws IOException {
    GetFeatureStatisticsRequest request = GetFeatureStatisticsRequest.newBuilder().build();

    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage(
        "Invalid request. Either provide dataset ids to retrieve statistics over, or a start date and end date.");
    statsService.getFeatureStatistics(request);
  }

  @Test
  public void shouldThrowExceptionIfInvalidDatesProvided() throws IOException {
    GetFeatureStatisticsRequest request =
        GetFeatureStatisticsRequest.newBuilder()
            .setStartDate(Timestamp.newBuilder().setSeconds(1))
            .setEndDate(Timestamp.newBuilder().setSeconds(0))
            .build();

    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage(
        "Invalid request. Start timestamp 1 is greater than the end timestamp 0");
    statsService.getFeatureStatistics(request);
  }

  @Test
  public void shouldThrowExceptionIfInvalidStoreProvided() throws IOException {
    GetFeatureStatisticsRequest request =
        GetFeatureStatisticsRequest.newBuilder()
            .setStartDate(Timestamp.newBuilder().setSeconds(0))
            .setEndDate(Timestamp.newBuilder().setSeconds(1))
            .setStore("redis")
            .build();

    when(storeRepository.findById("redis"))
        .thenReturn(
            Optional.of(
                Store.fromProto(
                    StoreProto.Store.newBuilder()
                        .setName("redis")
                        .setType(StoreType.REDIS)
                        .build())));

    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage(
        "Invalid store redis with type REDIS specified. Batch statistics are only supported for BigQuery stores");
    statsService.getFeatureStatistics(request);
  }

  @Test
  public void shouldThrowExceptionIfFeatureSetNotFound() throws IOException {
    GetFeatureStatisticsRequest request =
        GetFeatureStatisticsRequest.newBuilder()
            .setStartDate(Timestamp.newBuilder().setSeconds(0))
            .setEndDate(Timestamp.newBuilder().setSeconds(1))
            .setStore("bigquery")
            .setFeatureSetId("my_feature_set")
            .build();

    StoreProto.Store storeProto =
        StoreProto.Store.newBuilder()
            .setName("bigquery")
            .setType(StoreType.BIGQUERY)
            .setBigqueryConfig(
                BigQueryConfig.newBuilder().setProjectId("project").setDatasetId("dataset"))
            .build();
    when(storeRepository.findById("bigquery")).thenReturn(Optional.of(Store.fromProto(storeProto)));
    when(featureSetRepository.findFeatureSetByNameAndProject_Name(
            "my_feature_set", Project.DEFAULT_NAME))
        .thenReturn(null);
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("Illegal request. Unable to find feature set my_feature_set");
    statsService.getFeatureStatistics(request);
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
