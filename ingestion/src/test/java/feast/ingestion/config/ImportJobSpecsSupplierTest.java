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

import static org.junit.Assert.assertEquals;

import com.google.protobuf.util.JsonFormat;
import feast.ingestion.model.Specs;
import feast.ingestion.util.DateUtil;
import feast.specs.EntitySpecProto.EntitySpec;
import feast.specs.FeatureSpecProto.FeatureSpec;
import feast.specs.ImportJobSpecsProto.ImportJobSpecs;
import feast.specs.ImportSpecProto.Field;
import feast.specs.ImportSpecProto.ImportSpec;
import feast.specs.ImportSpecProto.Schema;
import feast.specs.StorageSpecProto.StorageSpec;
import feast.types.GranularityProto.Granularity;
import feast.types.ValueProto.ValueType;
import feast.types.ValueProto.ValueType.Enum;
import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class ImportJobSpecsSupplierTest {

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();
  String importSpecYaml =
      "---\n"
          + "servingStorageSpec:\n"
          + "  id: TEST_SERVING\n"
          + "  type: serving.mock\n"
          + "  options: {}\n"
          + "warehouseStorageSpec:\n"
          + "  id: TEST_WAREHOUSE\n"
          + "  type: warehouse.mock\n"
          + "  options: {}\n"
          + "entitySpecs:\n"
          + "  - name: testEntity\n"
          + "    description: This is a test entity\n"
          + "    tags: []\n"
          + "featureSpecs:\n"
          + "  - id: testEntity.day.testInt64\n"
          + "    entity: testEntity\n"
          + "    granularity: DAY\n"
          + "    name: testInt64\n"
          + "    owner: feast@example.com\n"
          + "    description: This is test feature of type integer\n"
          + "    uri: https://example.com/\n"
          + "    valueType: INT64\n"
          + "    tags: []\n"
          + "    options: {}\n"
          + "importSpec:\n"
          + "  type: file.csv\n"
          + "  sourceOptions:\n"
          + "    path: data.csv\n"
          + "  entities:\n"
          + "    - driver\n"
          + "  schema:\n"
          + "    entityIdColumn: driver_id\n"
          + "    timestampValue: 2018-09-25T00:00:00.000Z\n"
          + "    fields:\n"
          + "      - name: timestamp\n"
          + "      - name: driver_id\n"
          + "      - name: trips_completed\n"
          + "        featureId: driver.none.trips_completed\n"
          + "\n";

  @Test
  public void testSupplierImportSpecYamlFile() throws IOException {
    File yamlFile = temporaryFolder.newFile("importSpec.yaml");
    try (PrintWriter printWriter = new PrintWriter(Files.newOutputStream(yamlFile.toPath()))) {
      printWriter.print(importSpecYaml);
    }

    ImportJobSpecs importJobSpecs = new ImportJobSpecsSupplier(yamlFile.toString()).get();
    Specs specs = new Specs("", importJobSpecs);
    System.out
        .println(JsonFormat.printer().omittingInsignificantWhitespace().print(importJobSpecs));
    assertEquals(ImportSpec.newBuilder()
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
                        .setFeatureId("driver.none.trips_completed"))
                .setEntityIdColumn("driver_id")
                .setTimestampValue(DateUtil.toTimestamp("2018-09-25T00:00:00.000Z")))
        .build(), importJobSpecs.getImportSpec());

    assertEquals(StorageSpec.newBuilder()
        .setId("TEST_SERVING")
        .setType("serving.mock")
        .build(), importJobSpecs.getServingStorageSpec());

    assertEquals(StorageSpec.newBuilder()
        .setId("TEST_WAREHOUSE")
        .setType("warehouse.mock")
        .build(), importJobSpecs.getWarehouseStorageSpec());

    assertEquals(EntitySpec.newBuilder()
        .setName("testEntity")
        .setDescription("This is a test entity")
        .build(), specs.getEntitySpec("testEntity"));

    assertEquals(FeatureSpec.newBuilder()
        .setId("testEntity.day.testInt64")
        .setEntity("testEntity")
        .setName("testInt64")
        .setOwner("feast@example.com")
        .setUri("https://example.com/")
        .setValueType(Enum.INT64)
        .setGranularity(Granularity.Enum.DAY)
        .setDescription("This is test feature of type integer")
        .build(), specs.getFeatureSpec("testEntity.day.testInt64"));
  }
}
