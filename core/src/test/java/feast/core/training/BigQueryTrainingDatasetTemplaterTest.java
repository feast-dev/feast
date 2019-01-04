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
package feast.core.training;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.base.Charsets;
import com.google.common.io.CharStreams;
import com.google.protobuf.Timestamp;
import com.google.protobuf.util.Timestamps;
import com.hubspot.jinjava.Jinjava;
import feast.core.TrainingServiceProto.FeatureSet;
import feast.core.dao.FeatureInfoRepository;
import feast.core.model.EntityInfo;
import feast.core.model.FeatureInfo;
import feast.core.model.StorageInfo;
import feast.core.training.BigQueryTrainingDatasetTemplater.FeatureGroup;
import feast.specs.EntitySpecProto.EntitySpec;
import feast.specs.FeatureSpecProto.DataStore;
import feast.specs.FeatureSpecProto.DataStores;
import feast.specs.FeatureSpecProto.FeatureSpec;
import feast.specs.StorageSpecProto.StorageSpec;
import feast.types.GranularityProto.Granularity;
import feast.types.GranularityProto.Granularity.Enum;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import org.hibernate.engine.jdbc.internal.BasicFormatterImpl;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;

public class BigQueryTrainingDatasetTemplaterTest {
  private BigQueryTrainingDatasetTemplater templater;
  private BasicFormatterImpl formatter = new BasicFormatterImpl();

  @Mock private FeatureInfoRepository featureInfoRespository;
  private String sqlTemplate;

  @Before
  public void setUp() throws Exception {
    MockitoAnnotations.initMocks(this);

    Jinjava jinjava = new Jinjava();
    Resource resource = new ClassPathResource("templates/bq_training.tmpl");
    InputStream resourceInputStream = resource.getInputStream();
    sqlTemplate = CharStreams.toString(new InputStreamReader(resourceInputStream, Charsets.UTF_8));
    templater = new BigQueryTrainingDatasetTemplater(jinjava, sqlTemplate, featureInfoRespository);
  }

  @Test(expected = NoSuchElementException.class)
  public void shouldThrowNoSuchElementExceptionIfFeatureNotFound() {
    FeatureSet fs =
        FeatureSet.newBuilder()
            .setEntityName("myentity")
            .addAllFeatureIds(Arrays.asList("myentity.day.feature1", "myentity.day.feature2"))
            .build();
    templater.createQuery(fs, Timestamps.fromSeconds(0), Timestamps.fromSeconds(1), 0);
  }

  @Test
  public void shouldPassCorrectArgumentToTemplateEngine() {
    Jinjava jinjava = mock(Jinjava.class);
    templater = new BigQueryTrainingDatasetTemplater(jinjava, sqlTemplate, featureInfoRespository);

    Timestamp startDate =
        Timestamps.fromSeconds(Instant.parse("2018-01-01T00:00:00.00Z").getEpochSecond());
    Timestamp endDate =
        Timestamps.fromSeconds(Instant.parse("2019-01-01T00:00:00.00Z").getEpochSecond());
    int limit = 100;
    String featureId = "myentity.day.feature1";
    String tableId = "project.dataset.myentity_day";

    when(featureInfoRespository.findAllById(any(List.class)))
        .thenReturn(Collections.singletonList(createFeatureInfo(featureId, Enum.DAY, tableId)));

    FeatureSet fs =
        FeatureSet.newBuilder()
            .setEntityName("myentity")
            .addAllFeatureIds(Arrays.asList(featureId))
            .build();

    templater.createQuery(fs, startDate, endDate, limit);

    ArgumentCaptor<String> templateArg = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<Map<String, Object>> contextArg = ArgumentCaptor.forClass(Map.class);
    verify(jinjava).render(templateArg.capture(), contextArg.capture());

    String actualTemplate = templateArg.getValue();
    assertThat(actualTemplate, equalTo(sqlTemplate));

    Map<String, Object> actualContext = contextArg.getValue();
    assertThat(actualContext.get("start_date"), equalTo("2018-01-01"));
    assertThat(actualContext.get("end_date"), equalTo("2019-01-01"));
    assertThat(actualContext.get("limit"), equalTo(String.valueOf(limit)));

    List<FeatureGroup> featureGroups = (List<FeatureGroup>) actualContext.get("feature_groups");
    assertThat(featureGroups.size(), equalTo(1));
    assertThat(featureGroups.get(0).granularity, equalTo(Enum.DAY));
    assertThat(featureGroups.get(0).features.size(), equalTo(1));
    assertThat(featureGroups.get(0).features.get(0).featureId, equalTo(featureId));
    assertThat(featureGroups.get(0).features.get(0).granularity, equalTo(Enum.DAY));
    assertThat(featureGroups.get(0).features.get(0).tableId, equalTo(tableId));
  }

  @Test
  public void shouldRenderCorrectQuery1() throws Exception {
    String tableId1 = "project.dataset.myentity_day";
    String featureId1 = "myentity.day.feature1";
    String featureId2 = "myentity.day.feature2";

    FeatureInfo featureInfo1 = createFeatureInfo(featureId1, Enum.DAY, tableId1);
    FeatureInfo featureInfo2 = createFeatureInfo(featureId2, Enum.DAY, tableId1);

    String tableId2 = "project.dataset.myentity_none";
    String featureId3 = "myentity.none.feature3";
    FeatureInfo featureInfo3 = createFeatureInfo(featureId3, Enum.NONE, tableId2);

    when(featureInfoRespository.findAllById(any(List.class)))
        .thenReturn(Arrays.asList(featureInfo1, featureInfo2, featureInfo3));

    FeatureSet fs =
        FeatureSet.newBuilder()
            .setEntityName("myentity")
            .addAllFeatureIds(Arrays.asList(featureId1, featureId2, featureId3))
            .build();
    Timestamp startDate =
        Timestamps.fromSeconds(Instant.parse("2018-01-02T00:00:00.00Z").getEpochSecond());
    Timestamp endDate =
        Timestamps.fromSeconds(Instant.parse("2018-01-30T12:11:11.00Z").getEpochSecond());
    int limit = 100;

    String query = templater.createQuery(fs, startDate, endDate, limit);

    checkExpectedQuery(query, "expQuery1.sql");
  }

  @Test
  public void shouldRenderCorrectQuery2() throws Exception {
    String tableId1 = "project.dataset.myentity_day";
    String featureId1 = "myentity.day.feature1";
    String featureId2 = "myentity.day.feature2";

    FeatureInfo featureInfo1 = createFeatureInfo(featureId1, Enum.DAY, tableId1);
    FeatureInfo featureInfo2 = createFeatureInfo(featureId2, Enum.DAY, tableId1);

    String tableId2 = "project.dataset.myentity_none";
    String featureId3 = "myentity.none.feature3";
    FeatureInfo featureInfo3 = createFeatureInfo(featureId3, Enum.NONE, tableId2);

    when(featureInfoRespository.findAllById(any(List.class)))
        .thenReturn(Arrays.asList(featureInfo1, featureInfo2, featureInfo3));

    FeatureSet fs =
        FeatureSet.newBuilder()
            .setEntityName("myentity")
            .addAllFeatureIds(Arrays.asList(featureId1, featureId2, featureId3))
            .build();
    Timestamp startDate =
        Timestamps.fromSeconds(Instant.parse("2018-01-02T00:00:00.00Z").getEpochSecond());
    Timestamp endDate =
        Timestamps.fromSeconds(Instant.parse("2018-01-30T12:11:11.00Z").getEpochSecond());
    int limit = 0;

    String query = templater.createQuery(fs, startDate, endDate, limit);

    checkExpectedQuery(query, "expQuery2.sql");
  }

  @Test
  public void shouldRenderCorrectQuery3() throws Exception {
    List<FeatureInfo> featureInfos = new ArrayList<>();
    List<String> featureIds = new ArrayList<>();
    for (Granularity.Enum granularity : Granularity.Enum.values()) {
      if (granularity.equals(Enum.UNRECOGNIZED)) {
        continue;
      }

      String granularityStr = granularity.toString().toLowerCase();
      String tableId = "project.dataset.myentity_" + granularityStr;
      String featureId = "myentity." + granularityStr + ".feature1";

      featureInfos.add(createFeatureInfo(featureId, granularity, tableId));
      featureIds.add(featureId);
    }

    when(featureInfoRespository.findAllById(any(List.class))).thenReturn(featureInfos);

    Timestamp startDate =
        Timestamps.fromSeconds(Instant.parse("2018-01-02T00:00:00.00Z").getEpochSecond());
    Timestamp endDate =
        Timestamps.fromSeconds(Instant.parse("2018-01-30T12:11:11.00Z").getEpochSecond());
    FeatureSet featureSet =
        FeatureSet.newBuilder().setEntityName("myentity").addAllFeatureIds(featureIds).build();

    String query = templater.createQuery(featureSet, startDate, endDate, 1000);

    checkExpectedQuery(query, "expQuery3.sql");
  }

  private void checkExpectedQuery(String query, String pathToExpQuery) throws Exception {
    String tmpl =
        CharStreams.toString(
            new InputStreamReader(
                getClass().getClassLoader().getResourceAsStream("sql/" + pathToExpQuery),
                Charsets.UTF_8));

    String expQuery = formatter.format(tmpl.replaceAll("\\s+", " ").trim());
    query = formatter.format(query.replaceAll("\\s+", " ").trim());

    assertThat(query, equalTo(expQuery));
  }

  private FeatureInfo createFeatureInfo(String id, Granularity.Enum granularity, String tableId) {
    StorageSpec storageSpec =
        StorageSpec.newBuilder()
            .setId("BQ")
            .setType("bigquery")
            .putOptions("project", tableId.split("\\.")[0])
            .putOptions("dataset", tableId.split("\\.")[1])
            .build();
    StorageInfo storageInfo = new StorageInfo(storageSpec);

    FeatureSpec fs =
        FeatureSpec.newBuilder()
            .setId(id)
            .setGranularity(granularity)
            .setDataStores(DataStores.newBuilder().setWarehouse(DataStore.newBuilder().setId("BQ")))
            .build();

    EntitySpec entitySpec = EntitySpec.newBuilder().setName(id.split("\\.")[0]).build();
    EntityInfo entityInfo = new EntityInfo(entitySpec);
    return new FeatureInfo(fs, entityInfo, null, storageInfo, null);
  }
}
