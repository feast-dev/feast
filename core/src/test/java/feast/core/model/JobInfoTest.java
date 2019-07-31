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

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import feast.core.util.TypeConversion;
import feast.specs.EntitySpecProto.EntitySpec;
import feast.specs.FeatureSpecProto.FeatureSpec;
import feast.specs.ImportJobSpecsProto.ImportJobSpecs;
import feast.specs.ImportJobSpecsProto.SourceSpec;
import feast.specs.ImportJobSpecsProto.SourceSpec.SourceType;
import feast.specs.ImportSpecProto;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

public class JobInfoTest {

  @Test
  public void shouldInitialiseGivenJobIdAndSpec() throws InvalidProtocolBufferException {

    SourceSpec sourceSpec = SourceSpec.newBuilder()
        .setType(SourceType.KAFKA)
        .putOptions("bootstrapServers", "localhost:8281")
        .build();

    ImportJobSpecs importSpec = ImportJobSpecs.newBuilder()
        .setSourceSpec(sourceSpec)
        .setEntitySpec(EntitySpec.newBuilder().setName("entity").build())
        .addFeatureSpecs(FeatureSpec.newBuilder().setName("feature").build())
        .build();

    JobInfo actual = new JobInfo("fake-job-id", "fake-ext-id", "DataflowRunner", importSpec,
        JobStatus.PENDING);
    JobInfo expected = new JobInfo();
    expected.setId("fake-job-id");
    expected.setExtId("fake-ext-id");
    expected.setType("kafka");
    expected.setRunner("DataflowRunner");
    expected
        .setSourceOptions(
            TypeConversion.convertMapToJsonString(importSpec.getSourceSpec().getOptionsMap()));

    List<EntityInfo> entities = new ArrayList<>();
    EntityInfo entityInfo = new EntityInfo();
    entityInfo.setName("entity");
    entities.add(entityInfo);
    expected.setEntities(entities);

    List<FeatureInfo> features = new ArrayList<>();
    FeatureInfo featureInfo = new FeatureInfo();
    featureInfo.setName("feature");
    features.add(featureInfo);
    expected.setFeatures(features);

    expected.setRaw(JsonFormat.printer().print(importSpec));
    assertThat(actual.getId(), equalTo(expected.getId()));
    assertThat(actual.getExtId(), equalTo(expected.getExtId()));
    assertThat(actual.getType(), equalTo(expected.getType()));
    assertThat(actual.getRunner(), equalTo(expected.getRunner()));
    assertThat(actual.getEntities(), equalTo(expected.getEntities()));
    assertThat(actual.getFeatures(), equalTo(expected.getFeatures()));
    assertThat(actual.getSourceOptions(), equalTo(expected.getSourceOptions()));
    assertThat(actual.getRaw(), equalTo(expected.getRaw()));
  }
}