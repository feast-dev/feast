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

import com.google.common.collect.Lists;
import com.google.common.io.Resources;
import feast.ingestion.service.FileSpecService;
import feast.specs.FeatureSpecProto.DataStore;
import feast.specs.FeatureSpecProto.DataStores;
import feast.specs.FeatureSpecProto.FeatureSpec;
import feast.specs.ImportSpecProto.Field;
import feast.specs.ImportSpecProto.ImportSpec;
import feast.specs.ImportSpecProto.Schema;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import org.junit.Before;
import org.junit.Test;

public class SpecsTest {

  FileSpecService specService;

  private Field.Builder newField(String featureId) {
    return Field.newBuilder().setFeatureId(featureId);
  }

  @Before
  public void before() {
    Path path = Paths.get(Resources.getResource("core_specs/").getPath());
    specService = new FileSpecService(path.toString());
  }

  @Test
  public void testSingleFeatureAndEntity() {
    ImportSpec importSpec =
        ImportSpec.newBuilder()
            .addEntities("testEntity")
            .setSchema(Schema.newBuilder().addFields(newField("testEntity.none.testInt32")))
            .build();

    Specs specs = Specs.of("testjob", importSpec, specService);
    specs.validate();

    assertEquals("testjob", specs.getJobName());
    assertEquals(importSpec, specs.getImportSpec());

    assertEquals(1, specs.getEntitySpecs().size());
    assertTrue(specs.getEntitySpecs().containsKey("testEntity"));

    assertEquals(1, specs.getFeatureSpecs().size());
    assertTrue(specs.getFeatureSpecs().containsKey("testEntity.none.testInt32"));

    assertTrue(specs.getStorageSpecs().containsKey("TEST_SERVING"));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testErrorOnUnknownEntity() {
    ImportSpec importSpec =
        ImportSpec.newBuilder()
            .addEntities("testEntity")
            .setSchema(Schema.newBuilder().addFields(newField("testEntity.none.testInt32")))
            .build();

    Specs specs = Specs.of("testjob", importSpec, specService);
    specs.validate();

    specs.getEntitySpec("unknown");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testErrorOnUnknownFeature() {
    ImportSpec importSpec =
        ImportSpec.newBuilder()
            .addEntities("testEntity")
            .setSchema(Schema.newBuilder().addFields(newField("testEntity.none.testInt32")))
            .build();

    Specs specs = Specs.of("testjob", importSpec, specService);
    specs.validate();

    specs.getFeatureSpec("unknown");
  }

  @Test
  public void testGetFeatureSpec() {
    ImportSpec importSpec =
        ImportSpec.newBuilder()
            .addEntities("testEntity")
            .setSchema(Schema.newBuilder().addFields(newField("testEntity.none.testInt32")))
            .build();

    Specs specs = Specs.of("testjob", importSpec, specService);
    specs.validate();

    assertEquals(
        "testEntity.none.testInt32", specs.getFeatureSpec("testEntity.none.testInt32").getId());
  }

  @Test
  public void testGetEntitySpec() {
    ImportSpec importSpec =
        ImportSpec.newBuilder()
            .addEntities("testEntity")
            .setSchema(Schema.newBuilder().addFields(newField("testEntity.none.testInt32")))
            .build();

    Specs specs = Specs.of("testjob", importSpec, specService);
    specs.validate();

    assertEquals("testEntity", specs.getEntitySpec("testEntity").getName());
  }

  @Test
  public void testGetStorageSpec() {
    ImportSpec importSpec =
        ImportSpec.newBuilder()
            .addEntities("testEntity")
            .setSchema(Schema.newBuilder().addFields(newField("testEntity.none.testInt32")))
            .build();

    Specs specs = Specs.of("testjob", importSpec, specService);
    specs.validate();

    assertEquals("TEST_SERVING", specs.getStorageSpec("TEST_SERVING").getId());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testErrorOnUnknownStore() {
    ImportSpec importSpec =
        ImportSpec.newBuilder()
            .addEntities("testEntity")
            .setSchema(Schema.newBuilder().addFields(newField("testEntity.none.testInt32")))
            .build();

    Specs specs = Specs.of("testjob", importSpec, specService);
    specs.validate();

    specs.getStorageSpec("Unknown feature unknown, spec was not initialized");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testFeatureSpecReferencesUnknownEntity() {
    ImportSpec importSpec =
        ImportSpec.newBuilder()
            .addEntities("totally_different_entity")
            .setSchema(Schema.newBuilder().addFields(newField("testEntity.none.testInt32")))
            .build();

    Specs specs = Specs.of("testjob", importSpec, specService);
    specs.validate();
  }

  @Test
  public void testGetFeatureSpecByStoreId() {
    ImportSpec importSpec =
        ImportSpec.newBuilder()
            .addEntities("testEntity")
            .setSchema(
                Schema.newBuilder()
                    .addAllFields(
                        Lists.newArrayList(
                            newField("testEntity.none.testInt32").build(),
                            newField("testEntity.none.testString").build())))
            .build();

    Specs specs = Specs.of("testjob", importSpec, specService);
    specs.validate();

    FeatureSpec testStringSpec = specs.getFeatureSpec("testEntity.none.testString");
    DataStores dataStores =
        testStringSpec
            .getDataStores()
            .toBuilder()
            .setServing(DataStore.newBuilder().setId("differentStoreId"))
            .build();
    testStringSpec = testStringSpec.toBuilder().setDataStores(dataStores).build();

    // we change one of the specs to point at a different store id.
    specs.getFeatureSpecs().put("testEntity.none.testString", testStringSpec);

    List<FeatureSpec> featureSpecs1 = specs.getFeatureSpecByServingStoreId("TEST_SERVING");
    assertEquals(1, featureSpecs1.size());
    assertEquals("testEntity.none.testInt32", featureSpecs1.get(0).getId());


    List<FeatureSpec> featureSpecs2 = specs.getFeatureSpecByServingStoreId("differentStoreId");
    assertEquals(1, featureSpecs2.size());
    assertEquals(testStringSpec, featureSpecs2.get(0));
  }
}
