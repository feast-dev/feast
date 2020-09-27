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

import static feast.proto.types.ValueProto.ValueType.Enum.*;
import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

import feast.common.it.DataGenerator;
import feast.core.dao.EntityRepository;
import feast.core.util.TypeConversion;
import feast.proto.core.EntityProto;
import feast.proto.core.FeatureProto.FeatureSpecV2;
import feast.proto.core.FeatureTableProto.FeatureTableSpec;
import java.util.List;
import java.util.Map;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

public class FeatureTableTest {
  public static final String PROJECT_NAME = "project";
  @Mock EntityRepository entityRepo;

  @Before
  public void setup() {
    initMocks(this);
    when(entityRepo.findEntityByNameAndProject_Name("driver_id", PROJECT_NAME))
        .thenReturn(
            EntityV2.fromProto(
                EntityProto.Entity.newBuilder()
                    .setSpec(
                        DataGenerator.createEntitySpecV2("driver_id", "A driver.", INT64, Map.of()))
                    .build()));
  }

  @Test
  public void shouldStoreLabelsAsJson() {
    Map<String, String> expectedLabels = Map.of("test", "label");
    FeatureTableSpec spec = getTestSpec().toBuilder().putAllLabels(expectedLabels).build();

    FeatureTable table = FeatureTable.fromProto(PROJECT_NAME, spec, entityRepo);

    Map<String, String> actualLabels = TypeConversion.convertJsonStringToMap(table.getLabelsJSON());
    assertThat(actualLabels, equalTo(expectedLabels));
  }

  @Test
  public void shouldFromProtoBeReversableWithToProto() {
    FeatureTableSpec expectedSpec = getTestSpec();
    FeatureTableSpec actualSpec =
        FeatureTable.fromProto(PROJECT_NAME, expectedSpec, entityRepo).toProto().getSpec();
    assertThat(actualSpec, equalTo(expectedSpec));
  }

  @Test
  public void shouldUpdateFromSpec() {
    FeatureTable table = FeatureTable.fromProto(PROJECT_NAME, getTestSpec(), entityRepo);
    FeatureTableSpec expectedSpec =
        DataGenerator.createFeatureTableSpec(
                "driver",
                List.of("driver_id"),
                Map.of("ride_count", FLOAT, "avg_hops", INT64),
                3600,
                Map.of("test", "value"))
            .toBuilder()
            .setBatchSource(getTestSpec().getBatchSource())
            .setStreamSource(getTestSpec().getStreamSource())
            .build();

    table.updateFromProto(expectedSpec);
    FeatureTableSpec actualSpec = table.toProto().getSpec();

    assertThat(actualSpec, equalTo(expectedSpec));
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldErrorIfEntityChangeOnUpdate() {
    FeatureTable table = FeatureTable.fromProto(PROJECT_NAME, getTestSpec(), entityRepo);
    FeatureTableSpec badSpec =
        getTestSpec().toBuilder().addAllEntities(List.of("driver", "region")).build();
    table.updateFromProto(badSpec);
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldErrorIfFeatureValueTypeChangeOnUpdate() {
    FeatureTable table = FeatureTable.fromProto(PROJECT_NAME, getTestSpec(), entityRepo);
    FeatureTableSpec badSpec =
        getTestSpec()
            .toBuilder()
            .addAllFeatures(
                List.of(
                    FeatureSpecV2.newBuilder().setName("ride_count").setValueType(INT64).build()))
            .build();
    table.updateFromProto(badSpec);
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldErrorIfNameChangeOnUpdate() {
    FeatureTable table = FeatureTable.fromProto(PROJECT_NAME, getTestSpec(), entityRepo);
    FeatureTableSpec badSpec = getTestSpec().toBuilder().setName("driver_record").build();
    table.updateFromProto(badSpec);
  }

  public FeatureTableSpec getTestSpec() {
    FeatureTableSpec spec =
        DataGenerator.createFeatureTableSpec(
                "driver",
                List.of("driver_id"),
                Map.of("ride_count", FLOAT),
                3500,
                Map.of("test", "label"))
            .toBuilder()
            .setBatchSource(DataGenerator.createFileFeatureSourceSpec("file:///file"))
            .setStreamSource(
                DataGenerator.createKafkaFeatureSourceSpec("localhost:6566", "topc", "class.path"))
            .build();
    return spec;
  }
}
