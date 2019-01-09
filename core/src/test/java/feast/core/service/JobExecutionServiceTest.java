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

package feast.core.service;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;
import static org.mockito.internal.verification.VerificationModeFactory.times;

import com.google.common.collect.Lists;
import feast.core.config.ImportJobDefaults;
import feast.core.dao.JobInfoRepository;
import feast.core.model.JobInfo;
import feast.core.model.JobStatus;
import feast.specs.ImportSpecProto.ImportSpec;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;

public class JobExecutionServiceTest {

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();
  @Mock
  JobInfoRepository jobInfoRepository;
  private ImportJobDefaults defaults;

  @Before
  public void setUp() {
    initMocks(this);
    defaults =
        new ImportJobDefaults(
            "localhost:8080",
            "DirectRunner",
            "{\"key\":\"value\"}",
            "ingestion.jar",
            "STDOUT",
            "{}");
  }

  @Test
  public void shouldBuildProcessBuilderWithCorrectOptions() {
    JobExecutionService jobExecutionService = new JobExecutionService(jobInfoRepository, defaults);
    ImportSpec importSpec = ImportSpec.newBuilder().setType("file").build();
    ProcessBuilder pb = jobExecutionService.getProcessBuilder(importSpec, "test");
    List<String> expected =
        Lists.newArrayList(
            "java",
            "-jar",
            "ingestion.jar",
            "--jobName=test",
            "--runner=DirectRunner",
            "--importSpecBase64=CgRmaWxl",
            "--coreApiUri=localhost:8080",
            "--errorsStoreType=STDOUT",
            "--errorsStoreOptions={}",
            "--key=value");
    assertThat(pb.command(), equalTo(expected));
  }

  @Test
  public void shouldUpdateJobStatusIfExists() {
    JobInfo jobInfo = new JobInfo();
    when(jobInfoRepository.findById("jobid")).thenReturn(Optional.of(jobInfo));

    ArgumentCaptor<JobInfo> jobInfoArgumentCaptor = ArgumentCaptor.forClass(JobInfo.class);
    JobExecutionService jobExecutionService = new JobExecutionService(jobInfoRepository, defaults);
    jobExecutionService.updateJobStatus("jobid", JobStatus.PENDING);

    verify(jobInfoRepository, times(1)).saveAndFlush(jobInfoArgumentCaptor.capture());

    JobInfo jobInfoUpdated = new JobInfo();
    jobInfoUpdated.setStatus(JobStatus.PENDING);
    assertThat(jobInfoArgumentCaptor.getValue(), equalTo(jobInfoUpdated));
  }

  @Test
  public void shouldUpdateJobExtIdIfExists() {
    JobInfo jobInfo = new JobInfo();
    when(jobInfoRepository.findById("jobid")).thenReturn(Optional.of(jobInfo));

    ArgumentCaptor<JobInfo> jobInfoArgumentCaptor = ArgumentCaptor.forClass(JobInfo.class);
    JobExecutionService jobExecutionService = new JobExecutionService(jobInfoRepository, defaults);
    jobExecutionService.updateJobExtId("jobid", "extid");

    verify(jobInfoRepository, times(1)).saveAndFlush(jobInfoArgumentCaptor.capture());

    JobInfo jobInfoUpdated = new JobInfo();
    jobInfoUpdated.setExtId("extid");
    assertThat(jobInfoArgumentCaptor.getValue(), equalTo(jobInfoUpdated));
  }

  @Test
  public void shouldRunProcessAndGetJobIdIfNoError() throws IOException {
    Process process = Mockito.mock(Process.class);
    String processOutput = "log1: asdds\nlog2: dasdasd\nlog3: FeastImportJobId:1231231231\n";
    String errorOutput = "";
    InputStream outputStream =
        new ByteArrayInputStream(processOutput.getBytes(StandardCharsets.UTF_8));
    InputStream errorStream =
        new ByteArrayInputStream(errorOutput.getBytes(StandardCharsets.UTF_8));
    when(process.getInputStream()).thenReturn(outputStream);
    when(process.getErrorStream()).thenReturn(errorStream);
    when(process.exitValue()).thenReturn(0);
    when(process.isAlive()).thenReturn(true).thenReturn(false);
    JobExecutionService jobExecutionService = new JobExecutionService(jobInfoRepository, defaults);
    String jobId = jobExecutionService.runProcess(process);
    assertThat(jobId, equalTo("1231231231"));
  }

  @Test
  public void shouldThrowRuntimeExceptionIfErrorOccursInProcess() {
    Process process = Mockito.mock(Process.class);
    String processOutput = "log1: asdds\nlog2: dasdasd\n";
    String errorOutput = "error: stacktrace";
    InputStream outputStream =
        new ByteArrayInputStream(processOutput.getBytes(StandardCharsets.UTF_8));
    InputStream errorStream =
        new ByteArrayInputStream(errorOutput.getBytes(StandardCharsets.UTF_8));
    when(process.getInputStream()).thenReturn(outputStream);
    when(process.getErrorStream()).thenReturn(errorStream);
    when(process.exitValue()).thenReturn(1);
    when(process.isAlive()).thenReturn(true).thenReturn(false);
    expectedException.expect(RuntimeException.class);
    JobExecutionService jobExecutionService = new JobExecutionService(jobInfoRepository, defaults);
    jobExecutionService.runProcess(process);
  }
}
