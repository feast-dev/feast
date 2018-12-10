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

package feast.storage.file;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

import com.google.common.collect.Lists;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import feast.types.FeatureRowExtendedProto.FeatureRowExtended;
import feast.types.FeatureRowProto.FeatureRow;

public class TextFileFeatureIOTest {
  @Rule public TemporaryFolder folder = new TemporaryFolder();

  @Rule public TestPipeline testPipeline = TestPipeline.create();

  @Test
  public void testWrite() throws IOException {
    File path = folder.newFolder();

    FileStoreOptions options = new FileStoreOptions();
    options.jobName = testPipeline.getOptions().getJobName();
    options.path = path.getAbsolutePath();

    TextFileFeatureIO.Write write =
        new TextFileFeatureIO.Write(
            options, (featureRow) -> "output: " + featureRow.getRow().getEntityName(), ".text");

    PCollection<FeatureRowExtended> rowExtended =
        testPipeline.apply(
            Create.of(
                FeatureRowExtended.newBuilder()
                    .setRow(FeatureRow.newBuilder().setEntityName("testEntity"))
                    .build()));
    rowExtended.apply(write);

    testPipeline.run();

    List<Path> files = Files.walk(path.toPath()).collect(Collectors.toList());
    List<String> lines = Lists.newArrayList();
    for (Path file : files) {
      System.out.println(file);
      if (file.toFile().isFile()) {
        lines.addAll(Files.readAllLines(file));
      }
    }
    assertThat(lines, equalTo(Lists.newArrayList("output: testEntity")));
  }
}
