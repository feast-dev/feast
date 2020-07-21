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
import feast.proto.core.FeatureSetReferenceProto;
import feast.proto.types.ValueProto;
import java.util.Arrays;
import java.util.List;
import org.junit.Before;
import org.junit.Test;
import org.tensorflow.metadata.v0.*;

public class FeatureSetTest {

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
  public void shouldReturnFeatureSetStringRef() {
    FeatureSetSpec featureSetSpec =
        FeatureSetSpec.newBuilder()
            .setProject("project1")
            .setName("featureSetWithConstraints")
            .addAllEntities(entitySpecs)
            .addAllFeatures(featureSpecs)
            .build();

    FeatureSetReferenceProto.FeatureSetReference featureSetReference =
        FeatureSetReferenceProto.FeatureSetReference.newBuilder()
            .setName(featureSetSpec.getName())
            .setProject(featureSetSpec.getProject())
            .build();

    String actualFeatureSetStringRef1 = FeatureSet.getFeatureSetStringRef(featureSetSpec);
    String actualFeatureSetStringRef2 = FeatureSet.getFeatureSetStringRef(featureSetReference);
    String expectedFeatureSetStringRef = "project1/featureSetWithConstraints";

    assertThat(actualFeatureSetStringRef1, equalTo(expectedFeatureSetStringRef));
    assertThat(actualFeatureSetStringRef2, equalTo(expectedFeatureSetStringRef));
  }
}
