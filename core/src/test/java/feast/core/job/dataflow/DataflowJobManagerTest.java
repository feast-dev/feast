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

package feast.core.job.dataflow;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

import com.google.api.services.dataflow.Dataflow;
import com.google.common.collect.Lists;
import feast.core.config.ImportJobDefaults;
import feast.core.util.PathUtil;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.DefaultExecutor;
import org.apache.commons.exec.Executor;
import org.apache.logging.log4j.util.Strings;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mock;
import org.mockito.Mockito;

public class DataflowJobManagerTest {

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  @Mock
  Dataflow dataflow;

  @Rule
  public TemporaryFolder tempFolder = new TemporaryFolder();

  private ImportJobDefaults defaults;
  private DataflowJobManager dfJobManager;
  private Path workspace;

  @Before
  public void setUp() throws IOException {
    initMocks(this);
    workspace = Paths.get(tempFolder.newFolder().toString());
    defaults =
        ImportJobDefaults.builder()
            .runner("DataflowRunner")
            .importJobOptions("{\"key\":\"value\"}")
            .executable("ingestion.jar")
            .workspace(workspace.toString()).build();
    dfJobManager = new DataflowJobManager(dataflow, "project", "location", defaults);
  }

  @Test
  public void shouldBuildProcessBuilderWithCorrectOptions() {
    String jobName = "test";

    CommandLine cmdLine = dfJobManager.getCommandLine(jobName, Paths.get("/tmp/foobar"));
    List<String> expected =
        Lists.newArrayList(
            "-jar",
            "ingestion.jar",
            "--workspace=file:///tmp/foobar",
            "--jobName=test",
            "--runner=DataflowRunner",
            "--key=value");
    assertThat(cmdLine.getExecutable(), equalTo("java"));
    assertThat(cmdLine.getArguments(), equalTo(expected.toArray()));
  }

  @Test
  public void shouldBuildProcessBuilderWithGCSWorkspace() {

    String jobName = "test";

    CommandLine cmdLine = dfJobManager.getCommandLine(jobName, PathUtil.getPath("gs://bucket/tmp/foobar"));
    List<String> expected =
        Lists.newArrayList(
            "-jar",
            "ingestion.jar",
            "--workspace=gs://bucket/tmp/foobar",
            "--jobName=test",
            "--runner=DataflowRunner",
            "--key=value");
    assertThat(cmdLine.getExecutable(), equalTo("java"));
    assertThat(cmdLine.getArguments(), equalTo(expected.toArray()));
  }

  @Test
  public void shouldRunProcessAndGetJobIdIfNoError() throws IOException {
    CommandLine cmdLine = new CommandLine("echo");
    cmdLine.addArgument("12:59:19 [main] INFO  feast.ingestion.ImportJob - FeastImportJobId:2019-08-13_05_59_17-3784906597186500755", false);
    String jobId = dfJobManager.runProcess("myJob", cmdLine, new DefaultExecutor());
    assertThat(jobId, equalTo("2019-08-13_05_59_17-3784906597186500755"));
  }
}
