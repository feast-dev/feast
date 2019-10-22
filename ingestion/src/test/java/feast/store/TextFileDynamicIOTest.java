/*
 * Copyright 2019 The Feast Authors
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

package feast.store;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertThat;

import com.google.common.collect.Lists;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

@Slf4j
public class TextFileDynamicIOTest {

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

    TextFileDynamicIO.Write write =
        new TextFileDynamicIO.Write(
            options, ".text");

    PCollection<KV<String, String>> rowExtended =
        testPipeline.apply(Create.of(
            KV.of("part1", "line1"),
            KV.of("part1", "line2"),
            KV.of("part2", "line3"),
            KV.of("part2", "line4")));
    rowExtended.apply(write);

    testPipeline.run();

    List<String> part1 = getAllLines(path.toPath().resolve("test-job/part1"));
    List<String> part2 = getAllLines(path.toPath().resolve("test-job/part2"));
    assertThat(part1, containsInAnyOrder("line1", "line2"));
    assertThat(part2, containsInAnyOrder("line3", "line4"));
  }

  List<String> getAllLines(Path path) throws IOException {
    List<Path> files = Files.walk(path).collect(Collectors.toList());
    List<String> lines = Lists.newArrayList();
    for (Path file : files) {
      if (file.toFile().isFile()) {
        lines.addAll(Files.readAllLines(file));
      }
    }
    return lines;
  }
}
