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
import static org.junit.Assert.assertThat;

import com.google.protobuf.Timestamp;
import feast.core.UIServiceProto.UIServiceTypes.FeatureGroupDetail;
import feast.specs.FeatureGroupSpecProto.FeatureGroupSpec;
import java.util.Date;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class FeatureGroupInfoTest {

  @Rule
  public final ExpectedException exception = ExpectedException.none();
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

    featureGroupInfo.setOptions("{\"foo\":\"bar\"}");
    featureGroupSpec =
        FeatureGroupSpec.newBuilder()
            .setId("test")
            .addTags("tag1")
            .addTags("tag2")
            .putOptions("foo", "bar")
            .build();
  }

  @Test
  public void shouldBuildAndReturnCorrespondingSpec() {
    assertThat(featureGroupInfo.getFeatureGroupSpec(), equalTo(featureGroupSpec));
  }

  @Test
  public void shouldCorrectlyInitialiseFromGivenSpec() {
    assertThat(
        new FeatureGroupInfo(featureGroupSpec),
        equalTo(featureGroupInfo));
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
  public void shouldUpdateTags() {
    FeatureGroupSpec update =
        FeatureGroupSpec.newBuilder()
            .setId("test")
            .addTags("newtag")
            .putOptions("foo", "bar")
            .build();
    featureGroupInfo.update(update);

    FeatureGroupInfo expected = new FeatureGroupInfo(update);
    assertThat(featureGroupInfo, equalTo(expected));
  }

  @Test
  public void shouldThrowErrorIfOptionsChanged() {
    FeatureGroupSpec update =
        FeatureGroupSpec.newBuilder()
            .setId("test")
            .addTags("newtag")
            .putOptions("new", "option")
            .build();
    exception.expect(IllegalArgumentException.class);
    exception.expectMessage("Feature group already exists. Update only allowed for fields: [tags]");
    featureGroupInfo.update(update);
  }
}
