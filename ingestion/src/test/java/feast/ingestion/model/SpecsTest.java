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
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

import com.google.common.io.Resources;
import feast.ingestion.config.ImportJobSpecsSupplier;
import feast.specs.ImportJobSpecsProto.ImportJobSpecs;
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
        .setImportSpec(ImportSpec.newBuilder()
            .addEntities("testEntity")
            .setSchema(Schema.newBuilder().addFields(newField("testEntity.none.testInt32")))
        ).build();

    Specs specs = Specs.of("testjob", importJobSpecs);
    specs.validate();

    assertEquals("testjob", specs.getJobName());
    assertEquals(importJobSpecs.getImportSpec(), specs.getImportSpec());

    assertEquals(1, specs.getEntitySpecs().size());
    assertTrue(specs.getEntitySpecs().containsKey("testEntity"));

    assertEquals(1, specs.getFeatureSpecs().size());
    assertTrue(specs.getFeatureSpecs().containsKey("testEntity.none.testInt32"));

    assertTrue(specs.getServingStorageSpecs().containsKey("TEST_SERVING"));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testErrorOnUnknownEntity() {
    ImportJobSpecs importJobSpecs = this.importJobSpecs.toBuilder()
        .setImportSpec(ImportSpec.newBuilder()
            .addEntities("testEntity")
            .setSchema(Schema.newBuilder().addFields(newField("testEntity.none.testInt32")))
        ).build();

    Specs specs = Specs.of("testjob", importJobSpecs);
    specs.validate();

    specs.getEntitySpec("unknown");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testErrorOnUnknownFeature() {
    ImportJobSpecs importJobSpecs = this.importJobSpecs.toBuilder()
        .setImportSpec(ImportSpec.newBuilder()
            .addEntities("testEntity")
            .setSchema(Schema.newBuilder().addFields(newField("testEntity.none.testInt32")))
        ).build();

    Specs specs = Specs.of("testjob", importJobSpecs);
    specs.validate();

    specs.getFeatureSpec("unknown");
  }

  @Test
  public void testGetFeatureSpec() {
    ImportJobSpecs importJobSpecs = this.importJobSpecs.toBuilder()
        .setImportSpec(ImportSpec.newBuilder()
            .addEntities("testEntity")
            .setSchema(Schema.newBuilder().addFields(newField("testEntity.none.testInt32")))
        ).build();

    Specs specs = Specs.of("testjob", importJobSpecs);
    specs.validate();

    assertEquals(
        "testEntity.none.testInt32", specs.getFeatureSpec("testEntity.none.testInt32").getId());
  }

  @Test
  public void testGetEntitySpec() {
    ImportJobSpecs importJobSpecs = this.importJobSpecs.toBuilder()
        .setImportSpec(ImportSpec.newBuilder()
            .addEntities("testEntity")
            .setSchema(Schema.newBuilder().addFields(newField("testEntity.none.testInt32")))
        ).build();

    Specs specs = Specs.of("testjob", importJobSpecs);
    specs.validate();

    assertEquals("testEntity", specs.getEntitySpec("testEntity").getName());
  }

  @Test
  public void testGetStorageSpec() {
    ImportJobSpecs importJobSpecs = this.importJobSpecs.toBuilder()
        .setImportSpec(ImportSpec.newBuilder()
            .addEntities("testEntity")
            .setSchema(Schema.newBuilder().addFields(newField("testEntity.none.testInt32")))
        ).build();

    Specs specs = Specs.of("testjob", importJobSpecs);
    specs.validate();

    assertThat(specs.getWarehouseStorageSpecs().keySet(), containsInAnyOrder("TEST_WAREHOUSE"));
    assertThat(specs.getWarehouseStorageSpecs().keySet(), containsInAnyOrder("TEST_WAREHOUSE"));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testFeatureSpecReferencesUnknownEntity() {
    ImportJobSpecs importJobSpecs = this.importJobSpecs.toBuilder()
        .setImportSpec(ImportSpec.newBuilder()
            .addEntities("totally_different_entity")
            .setSchema(Schema.newBuilder().addFields(newField("testEntity.none.testInt32")))
        ).build();

    Specs specs = Specs.of("testjob", importJobSpecs);
    specs.validate();
  }
}
