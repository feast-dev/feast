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

import com.google.cloud.bigquery.BigQuery;
import com.google.protobuf.Timestamp;
import com.google.protobuf.util.Timestamps;
import feast.core.DatasetServiceProto.DatasetInfo;
import feast.core.DatasetServiceProto.FeatureSet;
import feast.core.storage.BigQueryStorageManager;
import feast.core.util.UuidProvider;
import feast.specs.StorageSpecProto.StorageSpec;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

// TODO: Should consider testing with "actual" BigQuery vs mocking it
//       because the mocked BigQuery client is very basic and may miss important functionalities
//       such as an actual table / dataset is actually created
//       In the test method, should probably add a condition so that tests can be skipped if
//       the user running the tests do not have permission to manage BigQuery (although ideally they should have)
//       Example of adding the condition whether or not to accept the test result as valid:
//       https://stackoverflow.com/questions/1689242/conditionally-ignoring-tests-in-junit-4

public class BigQueryTraningDatasetCreatorTest {

  public static final String projectId = "the-project";
  public static final String datasetPrefix = "feast";
  // class under test
  private BigQueryTraningDatasetCreator creator;
  @Mock
  private BigQueryDatasetTemplater templater;
  @Mock
  private BigQuery bq;
  @Mock
  private UuidProvider uuidProvider;

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
    when(templater.getStorageSpec()).thenReturn(StorageSpec.newBuilder()
        .setId("BIGQUERY1")
        .setType(BigQueryStorageManager.TYPE)
        .putOptions("project", "project")
        .putOptions("dataset", "dataset")
        .build());
    creator = new BigQueryTraningDatasetCreator(templater, projectId, datasetPrefix, uuidProvider, bq);

    when(uuidProvider.getUuid()).thenReturn("b0009f0f7df634ddc130571319e0deb9742eb1da");
    when(templater.createQuery(
        any(FeatureSet.class), any(Timestamp.class), any(Timestamp.class), anyLong(), anyMap()))
        .thenReturn("SELECT * FROM `project.dataset.table`");
  }

  @Test
  public void shouldCreateCorrectDatasetIfPrefixNotSpecified() {
    String entityName = "myentity";

    FeatureSet featureSet =
        FeatureSet.newBuilder()
            .setEntityName(entityName)
            .addAllFeatureIds(Arrays.asList("myentity.feature1", "myentity.feature2"))
            .build();

    Timestamp startDate =
        Timestamps.fromSeconds(Instant.parse("2018-01-01T10:15:30.00Z").getEpochSecond());
    Timestamp endDate =
        Timestamps.fromSeconds(Instant.parse("2019-01-01T10:15:30.00Z").getEpochSecond());
    long limit = 999;
    String namePrefix = "";

    DatasetInfo dsInfo = creator.createDataset(featureSet, startDate, endDate, limit, namePrefix, Collections
        .emptyMap());
    assertThat(
        dsInfo.getName(), equalTo("feast_myentity_b0009f0f7df634ddc130571319e0deb9742eb1da"));
    assertThat(
        dsInfo.getTableUrl(),
        equalTo(
            String.format(
                "%s.dataset.%s_%s_%s",
                projectId, datasetPrefix, entityName, "b0009f0f7df634ddc130571319e0deb9742eb1da")));
  }

  @Test
  public void shouldCreateCorrectDatasetIfPrefixIsSpecified() {
    String entityName = "myentity";

    FeatureSet featureSet =
        FeatureSet.newBuilder()
            .setEntityName(entityName)
            .addAllFeatureIds(Arrays.asList("myentity.feature1", "myentity.feature2"))
            .build();

    Timestamp startDate =
        Timestamps.fromSeconds(Instant.parse("2018-01-01T10:15:30.00Z").getEpochSecond());
    Timestamp endDate =
        Timestamps.fromSeconds(Instant.parse("2019-01-01T10:15:30.00Z").getEpochSecond());
    long limit = 999;
    String namePrefix = "mydataset";

    DatasetInfo dsInfo = creator.createDataset(featureSet, startDate, endDate, limit, namePrefix, Collections.emptyMap());
    assertThat(
        dsInfo.getTableUrl(),
        equalTo(
            String.format(
                "%s.dataset.%s_%s_%s_%s",
                projectId,
                datasetPrefix,
                entityName,
                namePrefix,
                "b0009f0f7df634ddc130571319e0deb9742eb1da")));
    assertThat(
        dsInfo.getName(),
        equalTo("feast_myentity_mydataset_b0009f0f7df634ddc130571319e0deb9742eb1da"));
  }

  @Test
  public void shouldPassArgumentToTemplater() {
    FeatureSet featureSet =
        FeatureSet.newBuilder()
            .setEntityName("myentity")
            .addAllFeatureIds(Arrays.asList("myentity.feature1", "myentity.feature2"))
            .build();

    Timestamp startDate = Timestamps.fromSeconds(0);
    Timestamp endDate = Timestamps.fromSeconds(1000);
    long limit = 999;
    String namePrefix = "";

    creator.createDataset(featureSet, startDate, endDate, limit, namePrefix, Collections.emptyMap());

    verify(templater).createQuery(featureSet, startDate, endDate, limit, Collections.emptyMap());
  }
}
