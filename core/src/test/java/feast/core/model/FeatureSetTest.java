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
package feast.core.model;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

import com.google.protobuf.Duration;
import com.google.protobuf.InvalidProtocolBufferException;
import feast.core.FeatureSetProto;
import feast.core.FeatureSetProto.EntitySpec;
import feast.core.FeatureSetProto.FeatureSetSpec;
import feast.core.FeatureSetProto.FeatureSetStatus;
import feast.core.FeatureSetProto.FeatureSpec;
import feast.core.SourceProto;
import feast.core.SourceProto.KafkaSourceConfig;
import feast.core.SourceProto.SourceType;
import feast.types.ValueProto.ValueType.Enum;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.tensorflow.metadata.v0.IntDomain;

public class FeatureSetTest {
  @Rule public final ExpectedException expectedException = ExpectedException.none();

  private FeatureSetProto.FeatureSet oldFeatureSetProto;

  @Before
  public void setUp() {
    SourceProto.Source oldSource =
        SourceProto.Source.newBuilder()
            .setType(SourceType.KAFKA)
            .setKafkaSourceConfig(
                KafkaSourceConfig.newBuilder()
                    .setBootstrapServers("kafka:9092")
                    .setTopic("mytopic"))
            .build();

    oldFeatureSetProto =
        FeatureSetProto.FeatureSet.newBuilder()
            .setSpec(
                FeatureSetSpec.newBuilder()
                    .setName("featureSet")
                    .setProject("project")
                    .setMaxAge(Duration.newBuilder().setSeconds(100))
                    .setSource(oldSource)
                    .addFeatures(
                        FeatureSpec.newBuilder().setName("feature1").setValueType(Enum.INT64))
                    .addFeatures(
                        FeatureSpec.newBuilder().setName("feature2").setValueType(Enum.STRING))
                    .addEntities(
                        EntitySpec.newBuilder().setName("entity").setValueType(Enum.STRING))
                    .build())
            .build();
  }

  @Test
  public void shouldUpdateFromProto() throws InvalidProtocolBufferException {
    SourceProto.Source newSource =
        SourceProto.Source.newBuilder()
            .setType(SourceType.KAFKA)
            .setKafkaSourceConfig(
                KafkaSourceConfig.newBuilder()
                    .setBootstrapServers("kafka:9092")
                    .setTopic("mytopic-changed"))
            .build();

    FeatureSetProto.FeatureSet newFeatureSetProto =
        FeatureSetProto.FeatureSet.newBuilder()
            .setSpec(
                FeatureSetSpec.newBuilder()
                    .setName("featureSet")
                    .setProject("project")
                    .setMaxAge(Duration.newBuilder().setSeconds(101))
                    .setSource(newSource)
                    .addFeatures(
                        FeatureSpec.newBuilder()
                            .setName("feature1")
                            .setValueType(Enum.INT64)
                            .setIntDomain(IntDomain.newBuilder().setMax(10).setMin(0)))
                    .addFeatures(
                        FeatureSpec.newBuilder().setName("feature3").setValueType(Enum.STRING))
                    .addEntities(
                        EntitySpec.newBuilder().setName("entity").setValueType(Enum.STRING))
                    .build())
            .build();

    FeatureSet actual = FeatureSet.fromProto(oldFeatureSetProto);
    actual.updateFromProto(newFeatureSetProto);

    FeatureSet expected = FeatureSet.fromProto(newFeatureSetProto);
    Feature archivedFeature =
        Feature.fromProto(
            FeatureSpec.newBuilder().setName("feature2").setValueType(Enum.STRING).build());
    archivedFeature.setArchived(true);
    expected.addFeature(archivedFeature);
    assertThat(actual, equalTo(expected));
  }

  @Test
  public void shouldNotUpdateIfNoChange() throws InvalidProtocolBufferException {
    FeatureSet actual = FeatureSet.fromProto(oldFeatureSetProto);
    actual.setStatus(FeatureSetStatus.STATUS_READY);
    actual.updateFromProto(oldFeatureSetProto);

    FeatureSet expected = FeatureSet.fromProto(oldFeatureSetProto);
    expected.setStatus(FeatureSetStatus.STATUS_READY);

    assertThat(actual, equalTo(expected));
  }

  @Test
  public void shouldThrowExceptionIfUpdateWithEntitiesChanged()
      throws InvalidProtocolBufferException {
    SourceProto.Source newSource =
        SourceProto.Source.newBuilder()
            .setType(SourceType.KAFKA)
            .setKafkaSourceConfig(
                KafkaSourceConfig.newBuilder()
                    .setBootstrapServers("kafka:9092")
                    .setTopic("mytopic-changed"))
            .build();

    FeatureSetProto.FeatureSet newFeatureSetProto =
        FeatureSetProto.FeatureSet.newBuilder()
            .setSpec(
                FeatureSetSpec.newBuilder()
                    .setName("featureSet")
                    .setProject("project")
                    .setMaxAge(Duration.newBuilder().setSeconds(101))
                    .setSource(newSource)
                    .addFeatures(
                        FeatureSpec.newBuilder()
                            .setName("feature1")
                            .setValueType(Enum.INT64)
                            .setIntDomain(IntDomain.newBuilder().setMax(10).setMin(0)))
                    .addFeatures(
                        FeatureSpec.newBuilder().setName("feature3").setValueType(Enum.STRING))
                    .addEntities(EntitySpec.newBuilder().setName("entity").setValueType(Enum.FLOAT))
                    .build())
            .build();

    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage(containsString("does not match existing set of entities"));
    FeatureSet existingFeatureSet = FeatureSet.fromProto(oldFeatureSetProto);
    existingFeatureSet.updateFromProto(newFeatureSetProto);
  }

  @Test
  public void shouldThrowExceptionIfUpdateWithFeatureTypesChanged()
      throws InvalidProtocolBufferException {
    SourceProto.Source newSource =
        SourceProto.Source.newBuilder()
            .setType(SourceType.KAFKA)
            .setKafkaSourceConfig(
                KafkaSourceConfig.newBuilder()
                    .setBootstrapServers("kafka:9092")
                    .setTopic("mytopic-changed"))
            .build();

    FeatureSetProto.FeatureSet newFeatureSetProto =
        FeatureSetProto.FeatureSet.newBuilder()
            .setSpec(
                FeatureSetSpec.newBuilder()
                    .setName("featureSet")
                    .setProject("project")
                    .setMaxAge(Duration.newBuilder().setSeconds(101))
                    .setSource(newSource)
                    .addFeatures(
                        FeatureSpec.newBuilder()
                            .setName("feature1")
                            .setValueType(Enum.INT64)
                            .setIntDomain(IntDomain.newBuilder().setMax(10).setMin(0)))
                    .addFeatures(
                        FeatureSpec.newBuilder().setName("feature2").setValueType(Enum.FLOAT))
                    .addEntities(
                        EntitySpec.newBuilder().setName("entity").setValueType(Enum.STRING))
                    .build())
            .build();

    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage(
        containsString(
            "You are attempting to change the type of feature feature2 from STRING to FLOAT."));
    FeatureSet existingFeatureSet = FeatureSet.fromProto(oldFeatureSetProto);
    existingFeatureSet.updateFromProto(newFeatureSetProto);
  }
}
