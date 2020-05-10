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

import com.google.protobuf.Duration;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import feast.proto.core.FeatureSetProto;
import feast.proto.serving.ServingAPIProto;
import feast.storage.api.retriever.FeatureSetRequest;
import feast.storage.api.retriever.HistoricalRetrievalResult;
import java.io.IOException;
import java.util.*;
import java.util.zip.GZIPInputStream;
import net.snowflake.client.jdbc.internal.amazonaws.auth.profile.ProfileCredentialsProvider;
import net.snowflake.client.jdbc.internal.amazonaws.services.s3.AmazonS3;
import net.snowflake.client.jdbc.internal.amazonaws.services.s3.AmazonS3ClientBuilder;
import net.snowflake.client.jdbc.internal.amazonaws.services.s3.AmazonS3URI;
import net.snowflake.client.jdbc.internal.amazonaws.services.s3.model.GetObjectRequest;
import net.snowflake.client.jdbc.internal.amazonaws.services.s3.model.S3Object;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.springframework.jdbc.core.JdbcTemplate;

@Ignore
public class SnowflakeHistoricalRetrieverTest {
  /** Manual test needs a testing Snowflake account */
  private SnowflakeHistoricalRetriever snowflakeFeatureRetriever;
  //  Snowflake account
  private String staging_location = "s3://feast-snowflake-staging/test/";

  private Map<String, String> snowflakeConfig = new HashMap<>();
  private String SFUrl = System.getenv("SNOWFLAKE_URL");
  private String SFClassName = "net.snowflake.client.jdbc.SnowflakeDriver";
  private String SFUsername = System.getenv("SNOWFLAKE_USERNAME");
  private String SFpw = System.getenv("SNOWFLAKE_PASSWORD");
  private String SFDatabase = "DEMO_DB";
  private String SFSchema = "PUBLIC";
  private String SFTable = "FEAST_FEATURES";
  private String SFRole = "ACCOUNTADMIN";
  private String SFStorageIntegration = "s3_int";

  @Before
  public void setUp() {

    snowflakeConfig.put("database", SFDatabase);
    snowflakeConfig.put("schema", SFSchema);
    snowflakeConfig.put("class_name", SFClassName);
    snowflakeConfig.put("username", SFUsername);
    snowflakeConfig.put("password", SFpw);
    snowflakeConfig.put("url", SFUrl);
    snowflakeConfig.put("staging_location", staging_location);
    snowflakeConfig.put("role", SFRole);
    snowflakeConfig.put("storage_integration", SFStorageIntegration);
    snowflakeConfig.put("table_name", SFTable);
    Properties dsProperties = new Properties();
    dsProperties.put("user", this.snowflakeConfig.get("username"));
    dsProperties.put("password", this.snowflakeConfig.get("password"));
    dsProperties.put("db", this.snowflakeConfig.get("database"));
    dsProperties.put("schema", this.snowflakeConfig.get("schema"));
    dsProperties.put("role", this.snowflakeConfig.get("role"));
    HikariConfig hkConfig = new HikariConfig();
    hkConfig.setMaximumPoolSize(100);
    hkConfig.setDriverClassName(SFClassName);
    hkConfig.setDataSourceProperties(dsProperties);
    hkConfig.setJdbcUrl(this.snowflakeConfig.get("url"));
    final HikariDataSource ds = new HikariDataSource(hkConfig);
    JdbcTemplate jdbcTemplate = new JdbcTemplate(ds);
    SnowflakeQueryTemplater snowflakeQueryTemplater =
        new SnowflakeQueryTemplater(snowflakeConfig, jdbcTemplate);
    snowflakeFeatureRetriever =
        (SnowflakeHistoricalRetriever)
            SnowflakeHistoricalRetriever.create(snowflakeConfig, snowflakeQueryTemplater);
  }

  @Test
  public void shouldRetrieveFromSnowflakeTest3DatesWithMaxAge() throws IOException {
    String entitySourceUri = "s3://feast-snowflake-staging/test/entity_tables/entities_3dates.csv";
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
    // get csv.gz file from s3
    List<String> resultLines = this.readFromS3(files.get(0));
    Assert.assertEquals("\\" + "\\N", resultLines.get(1).split(",")[3]);
    Assert.assertEquals("100", resultLines.get(2).split(",")[3]);
    Assert.assertEquals("300", resultLines.get(3).split(",")[3]);
  }

  @Test
  public void shouldRetrieveFromSnowflakeTest1Date() throws IOException {
    //      Set CSV format DATA_FORMAT_CSV = 2; where the first column of the csv file must be
    // entity_id
    //      file_uri is under
    // src/test/java/feast/storage/connectors/jdbc/retriever/entities_1date.csv
    String entitySourceUri = "s3://feast-snowflake-staging/test/entity_tables/entities_1date.csv";
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
    // get csv.gz file from s3
    List<String> resultLines = this.readFromS3(files.get(0));
    Assert.assertEquals("410", resultLines.get(1).split(",")[3]);
    Assert.assertEquals("220", resultLines.get(2).split(",")[3]);
    Assert.assertEquals("300", resultLines.get(3).split(",")[3]);
  }

  @Test
  public void shouldRetrieveFromSnowflakeTest1DateWithNull() throws IOException {
    //      Set CSV format DATA_FORMAT_CSV = 2; where the first column of the csv file must be
    // entity_id
    //      file_uri is under
    // src/test/java/feast/storage/connectors/jdbc/retriever/entities_1date_null.csv
    String entitySourceUri =
        "s3://feast-snowflake-staging/test/entity_tables/entities_1date_null.csv";
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
    /** Should return ENTITY_ID_PRIMARY, FEATURE_SET__FEATURE_1 1,410 2,100 3,null */
    Assert.assertTrue(files.get(0).contains(staging_location));
    // get csv.gz file from s3
    List<String> resultLines = this.readFromS3(files.get(0));
    Assert.assertEquals("410", resultLines.get(1).split(",")[3]);
    Assert.assertEquals("100", resultLines.get(2).split(",")[3]);
    Assert.assertEquals("\\" + "\\N", resultLines.get(3).split(",")[3]);
  }

  @Test
  public void shouldRetrieveFromSnowflakeTestSameIdsWithMaxAge() throws IOException {
    //      Set CSV format DATA_FORMAT_CSV = 2; where the first column of the csv file must be
    // entity_id
    //      file_uri is under
    // src/test/java/feast/storage/connectors/jdbc/retriever/entities_sameIDs.csv
    String file_uris = "s3://feast-snowflake-staging/test/entity_tables/entities_sameIDs.csv";
    ServingAPIProto.DatasetSource.FileSource fileSource =
        ServingAPIProto.DatasetSource.FileSource.newBuilder()
            .setDataFormatValue(2)
            .addFileUris(file_uris)
            .build();

    ServingAPIProto.DatasetSource datasetSource =
        ServingAPIProto.DatasetSource.newBuilder().setFileSource(fileSource).build();
    String retrievalId = "1234";
    List<FeatureSetRequest> featureSetRequests = this.createFeatureSetRequestsWithMaxAge();
    HistoricalRetrievalResult snowflakeHisRetrievalResult =
        snowflakeFeatureRetriever.getHistoricalFeatures(
            retrievalId, datasetSource, featureSetRequests, false);

    List<String> files = snowflakeHisRetrievalResult.getFileUris();
    /** Should return ENTITY_ID_PRIMARY, FEATURE_SET__FEATURE_1 1,null 2,100 3,300 1,410 */
    Assert.assertTrue(files.get(0).contains(staging_location));
    // get csv.gz file from s3
    List<String> resultLines = this.readFromS3(files.get(0));
    Assert.assertEquals("\\" + "\\N", resultLines.get(1).split(",")[3]);
    Assert.assertEquals("100", resultLines.get(2).split(",")[3]);
    Assert.assertEquals("300", resultLines.get(3).split(",")[3]);
    Assert.assertEquals("410", resultLines.get(4).split(",")[3]);
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

  /**
   * get the result feature set in s3 as a list of lines
   *
   * @param fileUri
   * @return
   * @throws IOException
   */
  private List<String> readFromS3(String fileUri) throws IOException {
    AmazonS3 s3client =
        AmazonS3ClientBuilder.standard().withCredentials(new ProfileCredentialsProvider()).build();
    String bucket = new AmazonS3URI(fileUri).getBucket();
    String key = new AmazonS3URI(fileUri).getKey();
    S3Object fileObj = s3client.getObject(new GetObjectRequest(bucket, key));
    Scanner fileIn = new Scanner(new GZIPInputStream(fileObj.getObjectContent()));
    List<String> resultLines = new ArrayList<>();
    if (null != fileIn) {
      while (fileIn.hasNext()) {
        resultLines.add(fileIn.nextLine());
      }
    }
    return resultLines;
  }
}
