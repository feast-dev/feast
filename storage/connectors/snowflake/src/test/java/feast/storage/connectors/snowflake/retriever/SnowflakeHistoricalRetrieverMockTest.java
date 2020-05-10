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
package feast.storage.connectors.snowflake.retriever;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doNothing;

import com.google.protobuf.Duration;
import feast.proto.core.FeatureSetProto;
import feast.proto.serving.ServingAPIProto;
import feast.storage.api.retriever.FeatureSetRequest;
import feast.storage.api.retriever.HistoricalRetrievalResult;
import feast.storage.connectors.snowflake.snowflake.TimestampLimits;
import java.sql.Timestamp;
import java.util.*;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.test.util.ReflectionTestUtils;

public class SnowflakeHistoricalRetrieverMockTest {
  private SnowflakeHistoricalRetriever snowflakeFeatureRetriever;
  private SnowflakeQueryTemplater snowflakeQueryTemplater;
  //  Snowflake account
  private String staging_location = "s3://feast-snowflake-staging/test/";
  private Map<String, String> snowflakeConfig = new HashMap<>();
  private String SFUrl = "jdbc:snowflake://fakeTesting.aws.snowflakecomputing.com";
  private String SFClassName = "net.snowflake.client.jdbc.SnowflakeDriver";
  private String SFusername = "fakeUsername";
  private String SFpw = "fakePassword";
  private String SFDatabase = "DEMO_DB";
  private String SFSchema = "PUBLIC";
  private String SFTable = "FEAST_FEATURES";
  private String SFRole = "ACCOUNTADMIN";
  private String SFStorageIntegration = "s3_int";
  @Mock JdbcTemplate jdbcTemplate;

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);

    snowflakeConfig.put("database", SFDatabase);
    snowflakeConfig.put("schema", SFSchema);
    snowflakeConfig.put("class_name", SFClassName);
    snowflakeConfig.put("username", SFusername);
    snowflakeConfig.put("password", SFpw);
    snowflakeConfig.put("url", SFUrl);
    snowflakeConfig.put("staging_location", staging_location);
    snowflakeConfig.put("role", SFRole);
    snowflakeConfig.put("storage_integration", SFStorageIntegration);
    snowflakeConfig.put("table_name", SFTable);
    snowflakeQueryTemplater = new SnowflakeQueryTemplater(snowflakeConfig, jdbcTemplate);
    snowflakeFeatureRetriever =
        (SnowflakeHistoricalRetriever)
            SnowflakeHistoricalRetriever.create(snowflakeConfig, snowflakeQueryTemplater);
  }

  @Test
  public void shouldRetrieveFromSnowflakeTest3DatesWithMaxAge() {
    String entitySourceUri = "s3://feast-snowflake-staging/test/entity_tables/entities_3dates.csv";
    // Mocking jdbcTemplate
    Timestamp minTime = Timestamp.valueOf("2019-12-31 17:00:00");
    Timestamp maxTime = Timestamp.valueOf("2020-01-01 10:00:00");
    TimestampLimits timestampLimits = new TimestampLimits(minTime, maxTime);

    ReflectionTestUtils.setField(snowflakeQueryTemplater, "jdbcTemplate", jdbcTemplate);
    ReflectionTestUtils.setField(
        snowflakeFeatureRetriever, "queryTemplater", snowflakeQueryTemplater);

    doNothing().when(jdbcTemplate).execute(any(String.class));
    Mockito.when(jdbcTemplate.queryForObject(any(String.class), any(RowMapper.class)))
        .thenReturn(timestampLimits);

    ServingAPIProto.DatasetSource.FileSource fileSource =
        ServingAPIProto.DatasetSource.FileSource.newBuilder()
            .setDataFormatValue(2)
            .addFileUris(entitySourceUri)
            .build();

    ServingAPIProto.DatasetSource datasetSource =
        ServingAPIProto.DatasetSource.newBuilder().setFileSource(fileSource).build();
    String retrievalId = "1234";
    List<FeatureSetRequest> featureSetRequests = this.createFeatureSetRequestsWithMaxAge();
    HistoricalRetrievalResult snowflakeHisRetrievalResult =
        snowflakeFeatureRetriever.getHistoricalFeatures(
            retrievalId, datasetSource, featureSetRequests, false);

    List<String> files = snowflakeHisRetrievalResult.getFileUris();
    /** Should return ENTITY_ID_PRIMARY, FEATURE_SET__FEATURE_1 1,null 2,100 3,300 */
    Assert.assertTrue(files.get(0).contains(staging_location));
  }

  @Test
  public void shouldRetrieveFromSnowflakeTest1Date() {
    String entitySourceUri = "s3://feast-snowflake-staging/test/entity_tables/entities_1date.csv";
    // Mocking jdbcTemplate
    Timestamp minTime = Timestamp.valueOf("2020-01-01 11:00:00");
    Timestamp maxTime = Timestamp.valueOf("2020-01-01 11:00:00");
    TimestampLimits timestampLimits = new TimestampLimits(minTime, maxTime);

    ReflectionTestUtils.setField(snowflakeQueryTemplater, "jdbcTemplate", jdbcTemplate);
    ReflectionTestUtils.setField(
        snowflakeFeatureRetriever, "queryTemplater", snowflakeQueryTemplater);

    doNothing().when(jdbcTemplate).execute(any(String.class));
    Mockito.when(jdbcTemplate.queryForObject(any(String.class), any(RowMapper.class)))
        .thenReturn(timestampLimits);
    ServingAPIProto.DatasetSource.FileSource fileSource =
        ServingAPIProto.DatasetSource.FileSource.newBuilder()
            .setDataFormatValue(2)
            .addFileUris(entitySourceUri)
            .build();

    ServingAPIProto.DatasetSource datasetSource =
        ServingAPIProto.DatasetSource.newBuilder().setFileSource(fileSource).build();

    String retrievalId = "1234";
    List<FeatureSetRequest> featureSetRequests = this.createFeatureSetRequests();
    HistoricalRetrievalResult snowflakeHisRetrievalResult =
        snowflakeFeatureRetriever.getHistoricalFeatures(
            retrievalId, datasetSource, featureSetRequests, false);

    List<String> files = snowflakeHisRetrievalResult.getFileUris();
    /** Should return ENTITY_ID_PRIMARY, FEATURE_SET__FEATURE_1 1,410 2,220 3,300 */
    Assert.assertTrue(files.get(0).contains(staging_location));
  }

  private List<FeatureSetRequest> createFeatureSetRequests() {
    FeatureSetRequest featureSetRequest =
        FeatureSetRequest.newBuilder()
            .setSpec(getFeatureSetSpec())
            .addFeatureReference(
                ServingAPIProto.FeatureReference.newBuilder()
                    .setName("feature_1")
                    .setProject("myproject4")
                    .setFeatureSet("feature_set")
                    .build())
            .addFeatureReference(
                ServingAPIProto.FeatureReference.newBuilder()
                    .setName("feature_2")
                    .setProject("myproject4")
                    .setFeatureSet("feature_set")
                    .build())
            .build();
    List<FeatureSetRequest> featureSetRequests = new ArrayList<>();
    featureSetRequests.add(featureSetRequest);
    return featureSetRequests;
  }

  private List<FeatureSetRequest> createFeatureSetRequestsWithMaxAge() {
    FeatureSetRequest featureSetRequest =
        FeatureSetRequest.newBuilder()
            .setSpec(getFeatureSetSpecWithMaxAge())
            .addFeatureReference(
                ServingAPIProto.FeatureReference.newBuilder()
                    .setName("feature_1")
                    .setProject("myproject4")
                    .setFeatureSet("feature_set")
                    .build())
            .addFeatureReference(
                ServingAPIProto.FeatureReference.newBuilder()
                    .setName("feature_2")
                    .setProject("myproject4")
                    .setFeatureSet("feature_set")
                    .build())
            .build();
    List<FeatureSetRequest> featureSetRequests = new ArrayList<>();
    featureSetRequests.add(featureSetRequest);
    return featureSetRequests;
  }

  private FeatureSetProto.FeatureSetSpec getFeatureSetSpecWithMaxAge() {
    return FeatureSetProto.FeatureSetSpec.newBuilder()
        .setProject("myproject4")
        .setName("feature_set")
        .addEntities(FeatureSetProto.EntitySpec.newBuilder().setName("entity_id_primary"))
        .addFeatures(FeatureSetProto.FeatureSpec.newBuilder().setName("feature_1"))
        .addFeatures(FeatureSetProto.FeatureSpec.newBuilder().setName("feature_2"))
        .setMaxAge(Duration.newBuilder().setSeconds(30)) // default
        .build();
  }

  private FeatureSetProto.FeatureSetSpec getFeatureSetSpec() {
    return FeatureSetProto.FeatureSetSpec.newBuilder()
        .setProject("myproject4")
        .setName("feature_set")
        .addEntities(FeatureSetProto.EntitySpec.newBuilder().setName("entity_id_primary"))
        .addFeatures(FeatureSetProto.FeatureSpec.newBuilder().setName("feature_1"))
        .addFeatures(FeatureSetProto.FeatureSpec.newBuilder().setName("feature_2"))
        .build();
  }
}
