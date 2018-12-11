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
import org.junit.Before;
import org.junit.Test;
import feast.core.UIServiceProto.UIServiceTypes.FeatureGroupDetail;
import feast.specs.FeatureGroupSpecProto.FeatureGroupSpec;
import feast.specs.FeatureSpecProto.DataStore;
import feast.specs.FeatureSpecProto.DataStores;

import java.time.Instant;
import java.util.Date;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

public class FeatureGroupInfoTest {
  private FeatureGroupInfo featureGroupInfo;
  private FeatureGroupSpec featureGroupSpec;
  private StorageInfo servingStorage;
  private StorageInfo warehouseStorage;

  @Before
  public void setUp() {
    servingStorage = new StorageInfo();
    servingStorage.setId("REDIS1");

    warehouseStorage = new StorageInfo();
    warehouseStorage.setId("REDIS2");

    featureGroupInfo = new FeatureGroupInfo();
    featureGroupInfo.setId("test");
    featureGroupInfo.setTags("tag1,tag2");
    featureGroupInfo.setServingStore(servingStorage);
    featureGroupInfo.setServingStoreOpts("{}");
    featureGroupInfo.setWarehouseStore(warehouseStorage);
    featureGroupInfo.setWarehouseStoreOpts("{}");

    DataStore servingStore = DataStore.newBuilder().setId("REDIS1").build();
    DataStore warehouseStore = DataStore.newBuilder().setId("REDIS2").build();
    DataStores dataStores =
            DataStores.newBuilder().setServing(servingStore).setWarehouse(warehouseStore).build();

    featureGroupSpec =
            FeatureGroupSpec.newBuilder()
                    .setId("test")
                    .addTags("tag1")
                    .addTags("tag2")
                    .setDataStores(dataStores)
                    .build();
  }

  @Test
  public void shouldBuildAndReturnCorrespondingSpec() {
    assertThat(featureGroupInfo.getFeatureGroupSpec(), equalTo(featureGroupSpec));
  }

  @Test
  public void shouldCorrectlyInitialiseFromGivenSpec() {
    assertThat(new FeatureGroupInfo(featureGroupSpec, servingStorage, warehouseStorage), equalTo(featureGroupInfo));
  }

  @Test
  public void shouldBuildAndReturnCorrespondingDetail() {
    featureGroupInfo.setLastUpdated(new Date(1000));
    Timestamp ts = Timestamp.newBuilder().setSeconds(1).build();
    FeatureGroupDetail expected =
            FeatureGroupDetail.newBuilder().setSpec(featureGroupSpec).setLastUpdated(ts).build();
    assertThat(featureGroupInfo.getFeatureGroupDetail(), equalTo(expected));
  }

  @Test
  public void shouldBeEqualToFeatureGroupFromSameSpecs() {
    FeatureGroupInfo featureGroup1 = new FeatureGroupInfo(featureGroupSpec, servingStorage, warehouseStorage);
    featureGroup1.setCreated(Date.from(Instant.ofEpochSecond(1)));
    FeatureGroupInfo featureGroup2 = new FeatureGroupInfo(featureGroupSpec, servingStorage, warehouseStorage);
    featureGroup2.setCreated(Date.from(Instant.ofEpochSecond(2)));
    assertThat(featureGroup1.eq(featureGroup2), equalTo(true));
  }
}
