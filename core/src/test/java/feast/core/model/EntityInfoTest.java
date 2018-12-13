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

import com.google.api.client.util.Lists;
import com.google.protobuf.Timestamp;
import org.junit.Before;
import org.junit.Test;
import feast.core.UIServiceProto.UIServiceTypes.EntityDetail;
import feast.specs.EntitySpecProto.EntitySpec;

import java.time.Instant;
import java.util.Date;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

public class EntityInfoTest {
  private EntityInfo entityInfo;
  private EntitySpec entitySpec;

  @Before
  public void setUp() {
    entityInfo = new EntityInfo();
    entityInfo.setName("test");
    entityInfo.setDescription("description");
    entityInfo.setTags("tag1,tag2");

    entitySpec =
            EntitySpec.newBuilder()
                    .setName("test")
                    .setDescription("description")
                    .addTags("tag1")
                    .addTags("tag2")
                    .build();
  }

  @Test
  public void shouldBuildAndReturnCorrespondingSpec() {
    assertThat(entityInfo.getEntitySpec(), equalTo(entitySpec));
  }

  @Test
  public void shouldCorrectlyInitialiseFromGivenSpec() {
    assertThat(new EntityInfo(entitySpec), equalTo(entityInfo));
  }

  @Test
  public void shouldBuildAndReturnCorrespondingDetail() {
    entityInfo.setLastUpdated(new Date(1000));
    Timestamp ts = Timestamp.newBuilder().setSeconds(1).build();
    EntityDetail expected =
            EntityDetail.newBuilder().setSpec(entitySpec).setLastUpdated(ts).build();
    assertThat(entityInfo.getEntityDetail(), equalTo(expected));
  }

  @Test
  public void shouldUpdateTagAndDescription() {
    EntityInfo entityInfo = new EntityInfo("entity", "test entity", "tag1,tag2", Lists.newArrayList(), false);
    EntitySpec update = EntitySpec.newBuilder().setName("entity").setDescription("overwrite").addTags("newtag").build();
    EntityInfo expected = new EntityInfo("entity", "overwrite", "newtag", Lists.newArrayList(), false);
    entityInfo.update(update);
    assertThat(entityInfo, equalTo(expected));
  }
}
