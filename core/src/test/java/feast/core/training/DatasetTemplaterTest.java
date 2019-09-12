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
import feast.core.DatasetServiceProto.FeatureSet;
import feast.core.dao.FeatureInfoRepository;
import feast.core.model.EntityInfo;
import feast.core.model.FeatureInfo;
import feast.core.storage.BigQueryStorageManager;
import feast.specs.EntitySpecProto.EntitySpec;
import feast.specs.FeatureSpecProto.FeatureSpec;
import feast.specs.StorageSpecProto.StorageSpec;
import feast.types.ValueProto.ValueType;
import feast.types.ValueProto.ValueType.Enum;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
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

public class DatasetTemplaterTest {

  private DatasetTemplater templater;
  private BasicFormatterImpl formatter = new BasicFormatterImpl();

  @Mock private FeatureInfoRepository featureInfoRespository;
  private String sqlTemplate;

  @Before
  public void setUp() throws Exception {
    MockitoAnnotations.initMocks(this);
    StorageSpec storageSpec =
        StorageSpec.newBuilder()
            .setId("BIGQUERY1")
            .setType(BigQueryStorageManager.TYPE)
            .putOptions("project", "project")
            .putOptions("dataset", "dataset")
            .build();

    Jinjava jinjava = new Jinjava();
    Resource resource = new ClassPathResource("templates/bq_training.tmpl");
    InputStream resourceInputStream = resource.getInputStream();
    sqlTemplate = CharStreams.toString(new InputStreamReader(resourceInputStream, Charsets.UTF_8));
    templater =
        new DatasetTemplater(jinjava, sqlTemplate, storageSpec, featureInfoRespository);
  }

  @Test(expected = NoSuchElementException.class)
  public void shouldThrowNoSuchElementExceptionIfFeatureNotFound() {
    FeatureSet fs =
        FeatureSet.newBuilder()
            .setEntityName("myentity")
            .addAllFeatureIds(Arrays.asList("myentity.feature1", "myentity.feature2"))
            .build();
    templater.createQuery(
        fs, Timestamps.fromSeconds(0), Timestamps.fromSeconds(1), 0, Collections.emptyMap());
  }

  @Test
  public void shouldPassCorrectArgumentToTemplateEngine() {
    StorageSpec storageSpec =
        StorageSpec.newBuilder()
            .setId("BIGQUERY1")
            .setType(BigQueryStorageManager.TYPE)
            .putOptions("project", "project")
            .putOptions("dataset", "dataset")
            .build();

    Jinjava jinjava = mock(Jinjava.class);
    templater =
        new DatasetTemplater(jinjava, sqlTemplate, storageSpec, featureInfoRespository);

    Timestamp startDate =
        Timestamps.fromSeconds(Instant.parse("2018-01-01T00:00:00.00Z").getEpochSecond());
    Timestamp endDate =
        Timestamps.fromSeconds(Instant.parse("2019-01-01T00:00:00.00Z").getEpochSecond());
    int limit = 100;
    String featureId = "myentity.feature1";
    String featureName = "feature1";

    when(featureInfoRespository.findAllById(any(List.class)))
        .thenReturn(Collections.singletonList(createFeatureInfo(featureId, featureName, Enum.INT64)));

    FeatureSet fs =
        FeatureSet.newBuilder()
            .setEntityName("myentity")
            .addAllFeatureIds(Arrays.asList(featureId))
            .build();

    templater.createQuery(fs, startDate, endDate, limit, Collections.emptyMap());

    ArgumentCaptor<String> templateArg = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<Map<String, Object>> contextArg = ArgumentCaptor.forClass(Map.class);
    verify(jinjava).render(templateArg.capture(), contextArg.capture());

    String actualTemplate = templateArg.getValue();
    assertThat(actualTemplate, equalTo(sqlTemplate));

    Map<String, Object> actualContext = contextArg.getValue();
    assertThat(actualContext.get("start_date"), equalTo("2018-01-01"));
    assertThat(actualContext.get("end_date"), equalTo("2019-01-01"));
    assertThat(actualContext.get("limit"), equalTo(String.valueOf(limit)));

    List<String> features = (List<String>) actualContext.get("features");
    assertThat(features.get(0), equalTo(featureName));
  }

  @Test
  public void shouldRenderCorrectQuery1() throws Exception {
    String featureId1 = "myentity.feature1";
    String featureName1 = "feature1";
    String featureId2 = "myentity.feature2";
    String featureName2 = "feature2";

    FeatureInfo featureInfo1 = createFeatureInfo(featureId1, featureName1, Enum.INT64);
    FeatureInfo featureInfo2 = createFeatureInfo(featureId2, featureName2, Enum.INT64);

    String featureId3 = "myentity.feature3";
    String featureName3 = "feature3";
    FeatureInfo featureInfo3 = createFeatureInfo(featureId3, featureName3, Enum.INT64);

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

    String query = templater.createQuery(fs, startDate, endDate, limit, Collections.emptyMap());

    checkExpectedQuery(query, "expQuery1.sql");
  }

  @Test
  public void shouldRenderCorrectQuery2() throws Exception {
    List<FeatureInfo> featureInfos = new ArrayList<>();
    List<String> featureIds = new ArrayList<>();

    String featureId = "myentity.feature1";
    String featureName = "feature1";

    featureInfos.add(createFeatureInfo(featureId, featureName, Enum.INT64));
    featureIds.add(featureId);

    when(featureInfoRespository.findAllById(any(List.class))).thenReturn(featureInfos);

    Timestamp startDate =
        Timestamps.fromSeconds(Instant.parse("2018-01-02T00:00:00.00Z").getEpochSecond());
    Timestamp endDate =
        Timestamps.fromSeconds(Instant.parse("2018-01-30T12:11:11.00Z").getEpochSecond());
    FeatureSet featureSet =
        FeatureSet.newBuilder().setEntityName("myentity").addAllFeatureIds(featureIds).build();

    String query =
        templater.createQuery(featureSet, startDate, endDate, 1000, Collections.emptyMap());

    checkExpectedQuery(query, "expQuery2.sql");
  }

  @Test
  public void shouldRenderCorrectQueryWithNumberFilter() throws Exception {
    List<FeatureInfo> featureInfos = new ArrayList<>();
    List<String> featureIds = new ArrayList<>();

    String featureId = "myentity.feature1";
    String featureId2 = "myentity.feature2";
    String featureName = "feature1";
    String featureName2 = "feature2";

    featureInfos.add(createFeatureInfo(featureId, featureName, Enum.INT64));
    featureInfos.add(createFeatureInfo(featureId2, featureName2, Enum.INT64));
    featureIds.add(featureId);
    featureIds.add(featureId2);

    when(featureInfoRespository.findAllById(any(List.class))).thenReturn(featureInfos);

    Timestamp startDate =
        Timestamps.fromSeconds(Instant.parse("2018-01-02T00:00:00.00Z").getEpochSecond());
    Timestamp endDate =
        Timestamps.fromSeconds(Instant.parse("2018-01-30T12:11:11.00Z").getEpochSecond());
    FeatureSet featureSet =
        FeatureSet.newBuilder().setEntityName("myentity").addAllFeatureIds(featureIds).build();

    Map<String, String> filter = new HashMap<>();
    filter.put("myentity.feature1", "10");

    String query =
        templater.createQuery(featureSet, startDate, endDate, 1000, filter);

    checkExpectedQuery(query, "expQueryWithNumberFilter.sql");
  }

  @Test
  public void shouldRenderCorrectQueryWithStringFilter() throws Exception {
    List<FeatureInfo> featureInfos = new ArrayList<>();
    List<String> featureIds = new ArrayList<>();

    String featureId = "myentity.feature1";
    String featureId2 = "myentity.feature2";
    String featureName = "feature1";
    String featureName2 = "feature2";

    featureInfos.add(createFeatureInfo(featureId, featureName, Enum.STRING));
    featureInfos.add(createFeatureInfo(featureId2, featureName2, Enum.STRING));
    featureIds.add(featureId);
    featureIds.add(featureId2);

    when(featureInfoRespository.findAllById(any(List.class))).thenReturn(featureInfos);

    Timestamp startDate =
        Timestamps.fromSeconds(Instant.parse("2018-01-02T00:00:00.00Z").getEpochSecond());
    Timestamp endDate =
        Timestamps.fromSeconds(Instant.parse("2018-01-30T12:11:11.00Z").getEpochSecond());
    FeatureSet featureSet =
        FeatureSet.newBuilder().setEntityName("myentity").addAllFeatureIds(featureIds).build();

    Map<String, String> filter = new HashMap<>();
    filter.put("myentity.feature1", "10");

    String query =
        templater.createQuery(featureSet, startDate, endDate, 1000, filter);

    checkExpectedQuery(query, "expQueryWithStringFilter.sql");
  }


  @Test
  public void shouldRenderCorrectQueryWithStringAndNumberFilter() throws Exception {
    List<FeatureInfo> featureInfos = new ArrayList<>();
    List<String> featureIds = new ArrayList<>();

    String featureId = "myentity.feature1";
    String featureId2 = "myentity.feature2";
    String featureName = "feature1";
    String featureName2 = "feature2";

    featureInfos.add(createFeatureInfo(featureId, featureName, Enum.INT64));
    featureInfos.add(createFeatureInfo(featureId2, featureName2, Enum.STRING));
    featureIds.add(featureId);
    featureIds.add(featureId2);

    when(featureInfoRespository.findAllById(any(List.class))).thenReturn(featureInfos);

    Timestamp startDate =
        Timestamps.fromSeconds(Instant.parse("2018-01-02T00:00:00.00Z").getEpochSecond());
    Timestamp endDate =
        Timestamps.fromSeconds(Instant.parse("2018-01-30T12:11:11.00Z").getEpochSecond());
    FeatureSet featureSet =
        FeatureSet.newBuilder().setEntityName("myentity").addAllFeatureIds(featureIds).build();

    Map<String, String> filter = new HashMap<>();
    filter.put("myentity.feature1", "10");
    filter.put("myentity.feature2", "HELLO");

    String query =
        templater.createQuery(featureSet, startDate, endDate, 1000, filter);

    checkExpectedQuery(query, "expQueryWithNumberAndStringFilter.sql");
  }


  @Test
  public void shouldRenderCorrectQueryWithJobIdFilter() throws Exception {
    List<FeatureInfo> featureInfos = new ArrayList<>();
    List<String> featureIds = new ArrayList<>();

    String featureId = "myentity.feature1";
    String featureId2 = "myentity.feature2";
    String featureName = "feature1";
    String featureName2 = "feature2";

    featureInfos.add(createFeatureInfo(featureId, featureName, Enum.INT64));
    featureInfos.add(createFeatureInfo(featureId2, featureName2, Enum.STRING));
    featureIds.add(featureId);
    featureIds.add(featureId2);

    when(featureInfoRespository.findAllById(any(List.class))).thenReturn(featureInfos);

    Timestamp startDate =
        Timestamps.fromSeconds(Instant.parse("2018-01-02T00:00:00.00Z").getEpochSecond());
    Timestamp endDate =
        Timestamps.fromSeconds(Instant.parse("2018-01-30T12:11:11.00Z").getEpochSecond());
    FeatureSet featureSet =
        FeatureSet.newBuilder().setEntityName("myentity").addAllFeatureIds(featureIds).build();

    Map<String, String> filter = new HashMap<>();
    filter.put("myentity.feature1", "10");
    filter.put("myentity.feature2", "HELLO");
    filter.put("job_id", "1234567890");

    String query =
        templater.createQuery(featureSet, startDate, endDate, 1000, filter);

    checkExpectedQuery(query, "expQueryWithJobIdFilter.sql");
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

  private FeatureInfo createFeatureInfo(String featureId, String featureName, ValueType.Enum valueType) {
    FeatureSpec fs =
        FeatureSpec.newBuilder()
            .setId(featureId)
            .setName(featureName)
            .setValueType(valueType)
            .build();

    EntitySpec entitySpec = EntitySpec.newBuilder().setName(featureId.split("\\.")[0]).build();
    EntityInfo entityInfo = new EntityInfo(entitySpec);
    return new FeatureInfo(fs, entityInfo, null);
  }
}
