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
import feast.core.UIServiceProto.UIServiceTypes.StorageDetail;
import feast.specs.StorageSpecProto.StorageSpec;
import org.junit.Before;
import org.junit.Test;

import java.time.Instant;
import java.util.Date;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

public class StorageInfoTest {
  private StorageInfo storageInfo;
  private StorageSpec storageSpec;

  @Before
  public void setUp() {
    storageInfo = new StorageInfo();
    storageInfo.setId("REDIS1");
    storageInfo.setType("redis");
    storageInfo.setOptions("{\"option1\":\"value\"}");

    storageSpec =
        StorageSpec.newBuilder()
            .setId("REDIS1")
            .setType("redis")
            .putOptions("option1", "value")
            .build();
  }

  @Test
  public void shouldBuildAndReturnCorrespondingSpec() {
    assertThat(storageInfo.getStorageSpec(), equalTo(storageSpec));
  }

  @Test
  public void shouldCorrectlyInitialiseFromGivenSpec() {
    assertThat(new StorageInfo(storageSpec), equalTo(storageInfo));
  }

  @Test
  public void shouldBuildAndReturnCorrespondingDetail() {
    storageInfo.setLastUpdated(new Date(1000));
    Timestamp ts = Timestamp.newBuilder().setSeconds(1).build();
    StorageDetail expected =
        StorageDetail.newBuilder().setSpec(storageSpec).setLastUpdated(ts).build();
    assertThat(storageInfo.getStorageDetail(), equalTo(expected));
  }

  @Test
  public void shouldBeEqualToStorageFromSameSpecs() {
    StorageInfo storage1 = new StorageInfo(storageSpec);
    storage1.setCreated(Date.from(Instant.ofEpochSecond(1)));
    StorageInfo storage2 = new StorageInfo(storageSpec);
    storage2.setCreated(Date.from(Instant.ofEpochSecond(2)));
    assertThat(storage1.eq(storage2), equalTo(true));
  }
}
