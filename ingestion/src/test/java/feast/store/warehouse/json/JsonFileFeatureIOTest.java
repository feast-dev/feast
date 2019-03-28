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

package feast.store.warehouse.json;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertThat;

import com.google.common.collect.Lists;
import feast.store.FileStoreOptions;
import feast.types.FeatureRowExtendedProto.FeatureRowExtended;
import feast.types.FeatureRowProto.FeatureRow;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

@Slf4j
public class JsonFileFeatureIOTest {

  @Rule
  public TemporaryFolder folder = new TemporaryFolder();

  @Rule
  public TestPipeline testPipeline = TestPipeline.create();

  @Test
  public void testWrite() throws IOException {
    File path = folder.newFolder();

    FileStoreOptions options = new FileStoreOptions();
    options.jobName = "test-job";
    options.path = path.getAbsolutePath();

    JsonFileWarehouseWrite write = new JsonFileWarehouseWrite(options);

    PCollection<FeatureRowExtended> rowExtended =
        testPipeline.apply(
            Create.of(
                FeatureRowExtended.newBuilder()
                    .setRow(
                        FeatureRow.newBuilder().setEntityName("testEntity1").setEntityKey("1234"))
                    .build(),
                FeatureRowExtended.newBuilder()
                    .setRow(
                        FeatureRow.newBuilder().setEntityName("testEntity2").setEntityKey("1234"))
                    .build()));
    rowExtended.apply(write);

    testPipeline.run();

    for (Path p : Files.walk(path.toPath()).collect(Collectors.toList())) {
      if (p.toFile().isFile()) {
        log.debug(p.toString());
      }
    }
    List<String> part1 = getAllLines(path.toPath().resolve("test-job/testEntity1"));
    List<String> part2 = getAllLines(path.toPath().resolve("test-job/testEntity2"));
    assertThat(part1,
        containsInAnyOrder("{\"entityKey\":\"1234\",\"entityName\":\"testEntity1\"}"));
    assertThat(part2,
        containsInAnyOrder("{\"entityKey\":\"1234\",\"entityName\":\"testEntity2\"}"));
  }

  List<String> getAllLines(Path path) throws IOException {
    List<Path> files = Files.walk(path).collect(Collectors.toList());
    List<String> lines = Lists.newArrayList();
    for (Path file : files) {
      System.out.println(file);
      if (file.toFile().isFile()) {
        lines.addAll(Files.readAllLines(file));
      }
    }
    return lines;
  }
}
