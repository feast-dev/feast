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
package feast.common.models;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;

import feast.proto.core.FeatureSetProto.EntitySpec;
import feast.proto.core.FeatureSetProto.FeatureSetSpec;
import feast.proto.core.FeatureSetProto.FeatureSpec;
import feast.proto.serving.ServingAPIProto;
import feast.proto.types.ValueProto;
import java.util.Arrays;
import java.util.List;
import org.junit.Before;
import org.junit.Test;
import org.tensorflow.metadata.v0.*;

public class FeaturesTest {

  private List<EntitySpec> entitySpecs;
  private List<FeatureSpec> featureSpecs;

  @Before
  public void setUp() {
    // Entity Specs
    EntitySpec entitySpec1 =
        EntitySpec.newBuilder()
            .setName("entity1")
            .setValueType(ValueProto.ValueType.Enum.INT64)
            .build();
    EntitySpec entitySpec2 =
        EntitySpec.newBuilder()
            .setName("entity2")
            .setValueType(ValueProto.ValueType.Enum.INT64)
            .build();

    // Feature Specs
    FeatureSpec featureSpec1 =
        FeatureSpec.newBuilder()
            .setName("feature1")
            .setValueType(ValueProto.ValueType.Enum.INT64)
            .setPresence(FeaturePresence.getDefaultInstance())
            .setShape(FixedShape.getDefaultInstance())
            .setDomain("mydomain")
            .build();
    FeatureSpec featureSpec2 =
        FeatureSpec.newBuilder()
            .setName("feature2")
            .setValueType(ValueProto.ValueType.Enum.INT64)
            .setGroupPresence(FeaturePresenceWithinGroup.getDefaultInstance())
            .setValueCount(ValueCount.getDefaultInstance())
            .setIntDomain(IntDomain.getDefaultInstance())
            .build();

    entitySpecs = Arrays.asList(entitySpec1, entitySpec2);
    featureSpecs = Arrays.asList(featureSpec1, featureSpec2);
  }

  @Test
  public void shouldReturnFeatureStringRef() {
    FeatureSetSpec featureSetSpec =
        FeatureSetSpec.newBuilder()
            .setProject("project1")
            .setName("featureSetWithConstraints")
            .addAllEntities(entitySpecs)
            .addAllFeatures(featureSpecs)
            .build();

    ServingAPIProto.FeatureReference featureReference =
        ServingAPIProto.FeatureReference.newBuilder()
            .setProject(featureSetSpec.getProject())
            .setFeatureSet(featureSetSpec.getName())
            .setName(featureSetSpec.getFeatures(0).getName())
            .build();

    String actualFeatureStringRef = Feature.getFeatureStringWithProjectRef(featureReference);
    String actualFeatureIgnoreProjectStringRef = Feature.getFeatureStringRef(featureReference);
    String expectedFeatureStringRef = "project1/featureSetWithConstraints:feature1";
    String expectedFeatureIgnoreProjectStringRef = "featureSetWithConstraints:feature1";

    assertThat(actualFeatureStringRef, equalTo(expectedFeatureStringRef));
    assertThat(actualFeatureIgnoreProjectStringRef, equalTo(expectedFeatureIgnoreProjectStringRef));
  }
}
