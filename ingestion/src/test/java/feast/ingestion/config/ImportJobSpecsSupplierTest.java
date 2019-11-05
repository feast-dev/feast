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

package feast.ingestion.config;

import feast.ingestion.model.Specs;
import feast.ingestion.util.DateUtil;
import feast.specs.EntitySpecProto.EntitySpec;
import feast.specs.FeatureSpecProto.FeatureSpec;
import feast.specs.ImportJobSpecsProto.ImportJobSpecs;
import feast.specs.ImportSpecProto.Field;
import feast.specs.ImportSpecProto.ImportSpec;
import feast.specs.ImportSpecProto.Schema;
import feast.specs.StorageSpecProto.StorageSpec;
import feast.types.ValueProto.ValueType.Enum;
import org.junit.Test;
import org.springframework.core.io.ClassPathResource;

import java.io.File;
import java.io.IOException;

import static org.hamcrest.Matchers.isEmptyOrNullString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

public class ImportJobSpecsSupplierTest {

  @Test
  public void testSupplierImportSpecYamlFile() throws IOException {
    File yamlFile = new ClassPathResource("ImportJobSpecsSupplierTest/testSupplierImportSpecYamlFile/importJobSpecs.yaml").getFile();

    ImportJobSpecs importJobSpecs = new ImportJobSpecsSupplier(yamlFile.getParent()).get();
    Specs specs = new Specs("", importJobSpecs);

    assertEquals(
        ImportSpec.newBuilder()
            .setType("file.csv")
            .putSourceOptions("path", "data.csv")
            .addEntities("driver")
            .setSchema(
                Schema.newBuilder()
                    .addFields(Field.newBuilder().setName("timestamp"))
                    .addFields(Field.newBuilder().setName("driver_id"))
                    .addFields(
                        Field.newBuilder()
                            .setName("trips_completed")
                            .setFeatureId("driver.trips_completed"))
                    .setEntityIdColumn("driver_id")
                    .setTimestampValue(DateUtil.toTimestamp("2018-09-25T00:00:00.000Z")))
            .build(),
        importJobSpecs.getImportSpec());

    assertEquals(StorageSpec.newBuilder()
        .setId("TEST_SERVING")
        .setType("serving.mock")
        .build(), importJobSpecs.getServingStorageSpec());

    assertEquals(StorageSpec.newBuilder()
        .setId("TEST_WAREHOUSE")
        .setType("warehouse.mock")
        .build(), importJobSpecs.getWarehouseStorageSpec());

    assertEquals(StorageSpec.newBuilder()
        .setId("ERRORS")
        .setType("stdout")
        .build(), importJobSpecs.getErrorsStorageSpec());

    assertEquals(
        EntitySpec.newBuilder()
            .setName("testEntity")
            .setDescription("This is a test entity")
            .build(),
        specs.getEntitySpec("testEntity"));

    assertEquals(
        FeatureSpec.newBuilder()
            .setId("testEntity.testInt64")
            .setEntity("testEntity")
            .setName("testInt64")
            .setOwner("feast@example.com")
            .setUri("https://example.com/")
            .setValueType(Enum.INT64)
            .setDescription("This is test feature of type integer")
            .build(),
        specs.getFeatureSpec("testEntity.testInt64"));
  }

  @Test
  public void testNoServingSpec() throws IOException {
    File yamlFile = new ClassPathResource("ImportJobSpecsSupplierTest/testNoStorageSpecs/importJobSpecs.yaml").getFile();

    ImportJobSpecs importJobSpecs = new ImportJobSpecsSupplier(yamlFile.getParent()).get();

    assertEquals(StorageSpec.getDefaultInstance(), importJobSpecs.getServingStorageSpec());
    assertThat(importJobSpecs.getServingStorageSpec().getId(), isEmptyOrNullString());
  }

  @Test
  public void testNoWarehouseSpec() throws IOException {
    File yamlFile = new ClassPathResource("ImportJobSpecsSupplierTest/testNoStorageSpecs/importJobSpecs.yaml").getFile();

    ImportJobSpecs importJobSpecs = new ImportJobSpecsSupplier(yamlFile.getParent()).get();

    assertEquals(StorageSpec.getDefaultInstance(), importJobSpecs.getWarehouseStorageSpec());
    assertThat(importJobSpecs.getWarehouseStorageSpec().getId(), isEmptyOrNullString());
  }

}
