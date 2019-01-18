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

import feast.ingestion.util.DateUtil;
import feast.ingestion.options.ImportJobPipelineOptions;
import com.google.protobuf.util.JsonFormat;
import feast.specs.ImportSpecProto.Field;
import feast.specs.ImportSpecProto.ImportSpec;
import feast.specs.ImportSpecProto.Schema;
import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.junit.Rule;
import static org.junit.Assert.*;

import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class ImportSpecSupplierTest {
  String importSpecYaml =
      "---\n"
          + "type: file\n"
          + "options:\n"
          + "  format: csv\n"
          + "  path: data.csv\n"
          + "entities:\n"
          + "  - driver\n"
          + "schema:\n"
          + "  entityIdColumn: driver_id\n"
          + "  timestampValue: 2018-09-25T00:00:00.000Z\n"
          + "  fields:\n"
          + "    - name: timestamp\n"
          + "    - name: driver_id\n"
          + "    - name: trips_completed\n"
          + "      featureId: driver.none.trips_completed\n"
          + "\n";

  String importSpecJson =
      "{\"type\":\"file\",\"options\":{\"format\":\"csv\",\"path\":\"data.csv\"},\"entities\":[\"driver\"],"
          + "\"schema\":"
          + "{\"fields\":[{\"name\":\"timestamp\"},"
          + "{\"name\":\"driver_id\"},{\"name\":\"trips_completed\",\"featureId\":\"driver.none.trips_completed\"}],\"timestampValue\":\"2018-09-25T00:00:00Z\",\"entityIdColumn\":\"driver_id\"}}\n";

  ImportSpec expectedImportSpec =
      ImportSpec.newBuilder()
          .setType("file")
          .putOptions("format", "csv")
          .putOptions("path", "data.csv")
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
          .build();

  @Rule public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void testSupplierImportSpecYamlFile() throws IOException {
    File yamlFile = temporaryFolder.newFile("importSpec.yaml");
    try (PrintWriter printWriter = new PrintWriter(Files.newOutputStream(yamlFile.toPath()))) {
      printWriter.print(importSpecYaml);
    }

    ImportJobPipelineOptions options = PipelineOptionsFactory.create().as(ImportJobPipelineOptions.class);
    options.setImportSpecYamlFile(yamlFile.toString());

    ImportSpec importSpec = new ImportSpecSupplier(options).get();
    System.out.println(JsonFormat.printer().omittingInsignificantWhitespace().print(importSpec));
    assertEquals(expectedImportSpec, importSpec);
  }
}
