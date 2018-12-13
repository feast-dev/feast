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

import com.google.protobuf.Timestamp;
import feast.core.UIServiceProto.UIServiceTypes.FeatureDetail;
import feast.specs.FeatureSpecProto.DataStore;
import feast.specs.FeatureSpecProto.DataStores;
import feast.specs.FeatureSpecProto.FeatureSpec;
import feast.types.GranularityProto.Granularity;
import feast.types.ValueProto.ValueType;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Date;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

public class FeatureInfoTest {
  private FeatureInfo featureInfo;
  private FeatureSpec featureSpec;
  private EntityInfo entityInfo;
  private StorageInfo servingStorage;
  private StorageInfo warehouseStorage;

  @Rule public final ExpectedException exception = ExpectedException.none();

  @Before
  public void setUp() {
    entityInfo = new EntityInfo();
    entityInfo.setName("entity");

    featureInfo = new FeatureInfo();
    featureInfo.setId("entity.NONE.name");
    featureInfo.setName("name");
    featureInfo.setOwner("owner");
    featureInfo.setDescription("desc");
    featureInfo.setUri("uri");
    featureInfo.setGranularity(Granularity.Enum.NONE);
    featureInfo.setValueType(ValueType.Enum.BYTES);
    featureInfo.setEntity(entityInfo);
    featureInfo.setOptions("{}");
    featureInfo.setTags("tag1,tag2");

    servingStorage = new StorageInfo();
    servingStorage.setId("REDIS1");

    warehouseStorage = new StorageInfo();
    warehouseStorage.setId("BIGQUERY");
    warehouseStorage.setType("bigquery");

    featureInfo.setServingStore(servingStorage);
    featureInfo.setServingStoreOpts("{}");
    featureInfo.setWarehouseStore(warehouseStorage);
    featureInfo.setWarehouseStoreOpts("{}");

    DataStore servingDataStore = DataStore.newBuilder().setId("REDIS1").build();
    DataStore warehouseDataStore = DataStore.newBuilder().setId("BIGQUERY").build();
    DataStores dataStores =
        DataStores.newBuilder()
            .setServing(servingDataStore)
            .setWarehouse(warehouseDataStore)
            .build();

    featureSpec =
        FeatureSpec.newBuilder()
            .setId("entity.NONE.name")
            .setName("name")
            .setOwner("owner")
            .setDescription("desc")
            .setEntity("entity")
            .setUri("uri")
            .setGranularity(Granularity.Enum.NONE)
            .setValueType(ValueType.Enum.BYTES)
            .addTags("tag1")
            .addTags("tag2")
            .setDataStores(dataStores)
            .build();
  }

  @Test
  public void shouldBuildAndReturnCorrespondingSpec() {
    assertThat(featureInfo.getFeatureSpec(), equalTo(featureSpec));
  }

  @Test
  public void shouldCorrectlyInitialiseFromGivenSpec() {
    assertThat(
        new FeatureInfo(featureSpec, entityInfo, servingStorage, warehouseStorage, null),
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
    assertThat(featureInfo.getFeatureDetail(), equalTo(expected));
  }

  @Test
  public void shouldBuildCorrespondingResolvedSpec() {
    FeatureGroupInfo featureGroupInfo = new FeatureGroupInfo();
    featureGroupInfo.setId("testGroup");
    featureGroupInfo.setServingStore(servingStorage);
    featureGroupInfo.setWarehouseStore(warehouseStorage);
    featureGroupInfo.setTags("inherited");
    FeatureInfo featureInfo = new FeatureInfo();
    featureInfo.setId("entity.NONE.name");
    featureInfo.setName("name");
    featureInfo.setOwner("owner");
    featureInfo.setDescription("desc");
    featureInfo.setUri("uri");
    featureInfo.setGranularity(Granularity.Enum.NONE);
    featureInfo.setValueType(ValueType.Enum.BYTES);
    featureInfo.setEntity(entityInfo);
    featureInfo.setOptions("{}");
    featureInfo.setTags("tag1,tag2");
    featureInfo.setFeatureGroup(featureGroupInfo);
    featureInfo.setServingStore(servingStorage);
    featureInfo.setWarehouseStore(warehouseStorage);

    DataStore servingDataStore = DataStore.newBuilder().setId("REDIS1").build();
    DataStore warehouseDataStore = DataStore.newBuilder().setId("BIGQUERY").build();
    DataStores dataStores =
        DataStores.newBuilder()
            .setServing(servingDataStore)
            .setWarehouse(warehouseDataStore)
            .build();

    FeatureSpec expected =
        FeatureSpec.newBuilder()
            .setId("entity.NONE.name")
            .setName("name")
            .setOwner("owner")
            .setDescription("desc")
            .setEntity("entity")
            .setUri("uri")
            .setGroup("testGroup")
            .setGranularity(Granularity.Enum.NONE)
            .setValueType(ValueType.Enum.BYTES)
            .addTags("tag1")
            .addTags("tag2")
            .addTags("inherited")
            .setDataStores(dataStores)
            .build();
    FeatureInfo resolved = featureInfo.resolve();
    assertThat(resolved.getFeatureSpec(), equalTo(expected));
  }

  @Test
  public void shouldUpdateMutableFields() {
    DataStore servingDataStore = DataStore.newBuilder().setId("REDIS1").build();
    DataStore warehouseDataStore = DataStore.newBuilder().setId("BIGQUERY").build();
    DataStores dataStores =
        DataStores.newBuilder()
            .setServing(servingDataStore)
            .setWarehouse(warehouseDataStore)
            .build();

    FeatureSpec update =
        FeatureSpec.newBuilder()
            .setId("entity.NONE.name")
            .setName("name")
            .setOwner("owner2")
            .setDescription("overwrite")
            .setEntity("entity")
            .setUri("new_uri")
            .setGranularity(Granularity.Enum.NONE)
            .setValueType(ValueType.Enum.BYTES)
            .addTags("new_tag")
            .setDataStores(dataStores)
            .build();
    featureInfo.update(featureSpec);
    FeatureInfo expected =
        new FeatureInfo(update, entityInfo, servingStorage, warehouseStorage, null);
    assertThat(featureInfo, equalTo(expected));
  }

  @Test
  public void shouldThrowExceptionIfImmutableFieldsChanged() {
    DataStore servingDataStore = DataStore.newBuilder().setId("REDIS2").build();
    DataStore warehouseDataStore = DataStore.newBuilder().setId("BIGQUERY").build();
    DataStores dataStores =
            DataStores.newBuilder()
                    .setServing(servingDataStore)
                    .setWarehouse(warehouseDataStore)
                    .build();

    FeatureSpec update =
            FeatureSpec.newBuilder()
                    .setId("entity.NONE.name")
                    .setName("name")
                    .setOwner("owner2")
                    .setDescription("overwrite")
                    .setEntity("entity")
                    .setUri("new_uri")
                    .setGranularity(Granularity.Enum.NONE)
                    .setValueType(ValueType.Enum.INT32)
                    .addTags("new_tag")
                    .setDataStores(dataStores)
                    .build();

    exception.expect(IllegalArgumentException.class);
    exception.expectMessage( "Feature already exists. Update only allowed for fields: [owner, description, uri, tags]");
    featureInfo.update(update);
  }


  @Test
  public void shouldThrowExceptionIfImmutableFieldsChangedToNull() {
    FeatureSpec update =
            FeatureSpec.newBuilder()
                    .setId("entity.NONE.name")
                    .setName("name")
                    .setOwner("owner2")
                    .setDescription("overwrite")
                    .setEntity("entity")
                    .setUri("new_uri")
                    .setGranularity(Granularity.Enum.NONE)
                    .setValueType(ValueType.Enum.BYTES)
                    .addTags("new_tag")
                    .build();

    exception.expect(IllegalArgumentException.class);
    exception.expectMessage( "Feature already exists. Update only allowed for fields: [owner, description, uri, tags]");
    featureInfo.update(update);
  }
}
