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

package feast.core.model;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

import com.google.protobuf.Timestamp;
import feast.core.UIServiceProto.UIServiceTypes.FeatureDetail;
import feast.core.config.StorageConfig.StorageSpecs;
import feast.specs.FeatureSpecProto.FeatureSpec;
import feast.specs.StorageSpecProto.StorageSpec;
import feast.types.ValueProto.ValueType;
import java.util.Date;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class FeatureInfoTest {

  @Rule
  public final ExpectedException exception = ExpectedException.none();
  private FeatureInfo featureInfo;
  private FeatureSpec featureSpec;
  private EntityInfo entityInfo;
  private StorageInfo servingStorage;
  private StorageInfo warehouseStorage;

  @Before
  public void setUp() {
    entityInfo = new EntityInfo();
    entityInfo.setName("entity");

    featureInfo = new FeatureInfo();
    featureInfo.setId("entity.name");
    featureInfo.setName("name");
    featureInfo.setOwner("owner");
    featureInfo.setDescription("desc");
    featureInfo.setUri("uri");
    featureInfo.setValueType(ValueType.Enum.BYTES);
    featureInfo.setEntity(entityInfo);
    featureInfo.setOptions("{}");
    featureInfo.setTags("tag1,tag2");

    servingStorage = new StorageInfo();
    servingStorage.setId("REDIS1");

    warehouseStorage = new StorageInfo();
    warehouseStorage.setId("BIGQUERY");
    warehouseStorage.setType("bigquery");

    featureSpec =
        FeatureSpec.newBuilder()
            .setId("entity.name")
            .setName("name")
            .setOwner("owner")
            .setDescription("desc")
            .setEntity("entity")
            .setUri("uri")
            .setValueType(ValueType.Enum.BYTES)
            .addTags("tag1")
            .addTags("tag2")
            .build();
  }

  @Test
  public void shouldBuildAndReturnCorrespondingSpec() {
    assertThat(featureInfo.getFeatureSpec(), equalTo(featureSpec));
  }

  @Test
  public void shouldCorrectlyInitialiseFromGivenSpec() {
    assertThat(
        new FeatureInfo(featureSpec, entityInfo, null),
        equalTo(featureInfo));
  }

  @Test
  public void shouldBuildAndReturnCorrespondingDetail() {
    featureInfo.setLastUpdated(new Date(1000));
    featureInfo.setCreated(new Date(1000));
    featureInfo.setBigQueryView("bqviewurl");
    Timestamp ts = Timestamp.newBuilder().setSeconds(1).build();
    FeatureDetail expected =
        FeatureDetail.newBuilder()
            .setSpec(featureSpec)
            .setBigqueryView("bqviewurl")
            .setEnabled(true)
            .setLastUpdated(ts)
            .setCreated(ts)
            .build();
    assertThat(featureInfo.getFeatureDetail(StorageSpecs.builder().build()), equalTo(expected));
  }

  @Test
  public void shouldBuildCorrespondingResolvedSpec() {
    FeatureGroupInfo featureGroupInfo = new FeatureGroupInfo();
    featureGroupInfo.setId("testGroup");
    featureGroupInfo.setTags("inherited");
    FeatureInfo featureInfo = new FeatureInfo();
    featureInfo.setId("entity.name");
    featureInfo.setName("name");
    featureInfo.setOwner("owner");
    featureInfo.setDescription("desc");
    featureInfo.setUri("uri");
    featureInfo.setValueType(ValueType.Enum.BYTES);
    featureInfo.setEntity(entityInfo);
    featureInfo.setOptions("{}");
    featureInfo.setTags("tag1,tag2");
    featureInfo.setFeatureGroup(featureGroupInfo);

    FeatureSpec expected =
        FeatureSpec.newBuilder()
            .setId("entity.name")
            .setName("name")
            .setOwner("owner")
            .setDescription("desc")
            .setEntity("entity")
            .setUri("uri")
            .setGroup("testGroup")
            .setValueType(ValueType.Enum.BYTES)
            .addTags("tag1")
            .addTags("tag2")
            .addTags("inherited")
            .build();
    FeatureInfo resolved = featureInfo.resolve();
    assertThat(resolved.getFeatureSpec(), equalTo(expected));
  }

  @Test
  public void shouldUpdateMutableFields() {
    FeatureSpec update =
        FeatureSpec.newBuilder()
            .setId("entity.name")
            .setName("name")
            .setOwner("owner2")
            .setDescription("overwrite")
            .setEntity("entity")
            .setUri("new_uri")
            .setValueType(ValueType.Enum.BYTES)
            .addTags("new_tag")
            .build();
    featureInfo.update(featureSpec);
    FeatureInfo expected =
        new FeatureInfo(update, entityInfo, null);
    assertThat(featureInfo, equalTo(expected));
  }

  @Test
  public void shouldThrowExceptionIfImmutableFieldsChanged() {
    FeatureSpec update =
            FeatureSpec.newBuilder()
                    .setId("entity.name")
                    .setName("name")
                    .setOwner("owner2")
                    .setDescription("overwrite")
                    .setEntity("entity")
                    .setUri("new_uri")
                    .setValueType(ValueType.Enum.INT32)
                    .addTags("new_tag")
                    .build();

    exception.expect(IllegalArgumentException.class);
    exception.expectMessage(
        "Feature already exists. Update only allowed for fields: [owner, description, uri, tags]");
    featureInfo.update(update);
  }


  @Test
  public void shouldThrowExceptionIfImmutableFieldsChangedToNull() {
    FeatureSpec update =
            FeatureSpec.newBuilder()
                    .setId("entity.name")
                    .setName("name")
                    .setOwner("owner2")
                    .setDescription("overwrite")
                    .setEntity("entity")
                    .setUri("new_uri")
                    .setValueType(ValueType.Enum.BYTES)
                    .addTags("new_tag")
                    .build();

    exception.expect(IllegalArgumentException.class);
    exception.expectMessage(
        "Feature already exists. Update only allowed for fields: [owner, description, uri, tags]");
    featureInfo.update(update);
  }

  @Test
  public void createBigQueryLink_withBigQueryType_shouldGenerateLink() {
    String link = featureInfo.createBigqueryViewLink(StorageSpec.newBuilder()
        .setType("bigquery").setId("BQ").putOptions("project", "project1")
        .putOptions("dataset", "dataset1").build());
    assertEquals(link, "https://bigquery.cloud.google.com/table/project1:dataset1.entity_none_view");
  }

  @Test
  public void createBigQueryLink_withOtherType_shouldNotGenerateLink() {
    String link = featureInfo.createBigqueryViewLink(StorageSpec.newBuilder()
        .setType("another_type").build());
    assertEquals(link, "N.A.");
  }

  @Test
  public void createBigQueryLink_withNullSpec_shouldNotGenerateLink() {
    String link = featureInfo.createBigqueryViewLink(null);
    assertEquals(link, "N.A.");
  }
}
