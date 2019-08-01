/*
 * Copyright 2019 The Feast Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package feast.ingestion.model;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;

import com.google.common.io.Resources;
import feast.ingestion.config.ImportJobSpecsSupplier;
import feast.specs.EntitySpecProto.EntitySpec;
import feast.specs.FeatureSpecProto.FeatureSpec;
import feast.specs.ImportJobSpecsProto.ImportJobSpecs;
import feast.specs.ImportJobSpecsProto.SourceSpec;
import feast.specs.ImportJobSpecsProto.SourceSpec.SourceType;
import feast.specs.ImportSpecProto.Field;
import feast.specs.ImportSpecProto.ImportSpec;
import feast.specs.ImportSpecProto.Schema;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.junit.Before;
import org.junit.Test;

public class SpecsTest {

  ImportJobSpecs importJobSpecs;

  private Field.Builder newField(String featureId) {
    return Field.newBuilder().setFeatureId(featureId);
  }

  @Before
  public void before() {
    Path path = Paths.get(Resources.getResource("specs/").getPath());
    importJobSpecs = new ImportJobSpecsSupplier(path.toString()).get();
  }

  @Test
  public void testSingleFeatureAndEntity() {
    ImportJobSpecs importJobSpecs = this.importJobSpecs.toBuilder()
        .setSourceSpec(SourceSpec.newBuilder().setType(SourceType.KAFKA)
            .putOptions("bootstrapServers", "localhost:8281"))
        .clearFeatureSpecs()
        .addFeatureSpecs(FeatureSpec.newBuilder().setId("testEntity.testInt32").build())
        .build();

    Specs specs = Specs.of("testjob", importJobSpecs);

    assertEquals("testjob", specs.getJobName());
    assertEquals(importJobSpecs.getSourceSpec(), specs.getSourceSpec());

    assertEquals(1, specs.getEntitySpecs().size());
    assertTrue(specs.getEntitySpecs().containsKey("testEntity"));

    assertEquals(1, specs.getFeatureSpecs().size());
    assertTrue(specs.getFeatureSpecs().containsKey("testEntity.testInt32"));

    assertTrue(specs.getSinkStorageSpec().getId().equals("TEST_SERVING"));
  }

  @Test
  public void testGetFeatureSpec() {
    Specs specs = Specs.of("testjob", importJobSpecs);
    assertEquals(
        "testEntity.testInt32", specs.getFeatureSpec("testEntity.testInt32").getId());
  }

  @Test
  public void testGetEntitySpec() {
    Specs specs = Specs.of("testjob", importJobSpecs);
    assertEquals("testEntity", specs.getEntitySpec("testEntity").getName());
  }

  @Test
  public void testGetStorageSpec() {
    Specs specs = Specs.of("testjob", importJobSpecs);
    assertEquals(specs.getSinkStorageSpec().getId(), "TEST_SERVING");
  }

}
