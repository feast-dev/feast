/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright 2018-2020 The Feast Authors
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
 */
package feast.core.service;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.beans.HasPropertyWithValue.hasProperty;
import static org.hamcrest.collection.IsCollectionWithSize.hasSize;
import static org.hamcrest.collection.IsIterableContainingInAnyOrder.containsInAnyOrder;

import com.google.common.collect.ImmutableMap;
import feast.common.models.FeatureSetReference;
import feast.core.it.BaseIT;
import feast.core.it.DataGenerator;
import feast.core.job.JobManager;
import feast.core.job.JobRepository;
import feast.core.model.FeatureSetDeliveryStatus;
import feast.core.model.Job;
import feast.core.model.JobStatus;
import feast.proto.core.CoreServiceGrpc;
import feast.proto.core.CoreServiceProto;
import feast.proto.core.FeatureSetReferenceProto;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;

@SpringBootTest()
public class JobServiceIT extends BaseIT {
  @Autowired private FakeJobManager jobManager;

  @Autowired private JobRepository jobRepository;

  static CoreServiceGrpc.CoreServiceBlockingStub stub;

  Job job;

  @BeforeAll
  public static void globalSetUp(@Value("${grpc.server.port}") int port) {
    ManagedChannel channel =
        ManagedChannelBuilder.forAddress("localhost", port).usePlaintext().build();
    stub = CoreServiceGrpc.newBlockingStub(channel);
  }

  private Job createJobWithId(String jobId) {
    return Job.builder()
        .setId(jobId)
        .setSource(DataGenerator.getDefaultSource())
        .setStores(
            ImmutableMap.of(
                DataGenerator.getDefaultStore().getName(), DataGenerator.getDefaultStore()))
        .build();
  }

  @BeforeEach
  public void createJob() {
    this.job = createJobWithId("some-id");
    this.jobManager.startJob(job);
    this.jobRepository.add(job);
  }

  @AfterEach
  public void tearDown() {
    this.jobManager.cleanAll();
    this.jobRepository.deleteAll();
  }

  @Test
  public void shouldReturnListOfJobsById() {
    CoreServiceProto.ListIngestionJobsRequest.Filter filter =
        CoreServiceProto.ListIngestionJobsRequest.Filter.newBuilder()
            .setId(this.job.getId())
            .build();

    assertReturnsJob(filter);
    CoreServiceProto.ListIngestionJobsRequest request;

    // list with no filter
    request = CoreServiceProto.ListIngestionJobsRequest.newBuilder().build();
    assertThat(
        stub.listIngestionJobs(request).getJobsList(),
        containsInAnyOrder(hasProperty("id", equalTo(this.job.getId()))));

    // list with empty filter
    filter = CoreServiceProto.ListIngestionJobsRequest.Filter.newBuilder().build();
    assertReturnsJob(filter);
  }

  @Test
  public void shouldReturnListOfJobsByStoreName() {
    CoreServiceProto.ListIngestionJobsRequest.Filter filter =
        CoreServiceProto.ListIngestionJobsRequest.Filter.newBuilder()
            .setStoreName(DataGenerator.getDefaultStore().getName())
            .build();

    assertReturnsJob(filter);
  }

  private void assertReturnsJob(CoreServiceProto.ListIngestionJobsRequest.Filter filter) {
    CoreServiceProto.ListIngestionJobsRequest request =
        CoreServiceProto.ListIngestionJobsRequest.newBuilder().setFilter(filter).build();

    assertThat(
        stub.listIngestionJobs(request).getJobsList(),
        containsInAnyOrder(hasProperty("id", equalTo(this.job.getId()))));
  }

  @Test
  public void shouldReturnListOfJobsByByFeatureSetReference() {
    FeatureSetReference ref = FeatureSetReference.of("default", "fs");
    FeatureSetDeliveryStatus featureSetDeliveryStatus = new FeatureSetDeliveryStatus();

    this.job.getFeatureSetDeliveryStatuses().put(ref, featureSetDeliveryStatus);

    // list job by feature set reference: name and project
    CoreServiceProto.ListIngestionJobsRequest.Filter filter =
        CoreServiceProto.ListIngestionJobsRequest.Filter.newBuilder()
            .setFeatureSetReference(
                FeatureSetReferenceProto.FeatureSetReference.newBuilder()
                    .setProject(ref.getProjectName())
                    .setName(ref.getFeatureSetName())
                    .build())
            .build();

    assertReturnsJob(filter);

    // list job by feature set reference: name
    filter =
        CoreServiceProto.ListIngestionJobsRequest.Filter.newBuilder()
            .setFeatureSetReference(
                FeatureSetReferenceProto.FeatureSetReference.newBuilder()
                    .setName(ref.getFeatureSetName())
                    .build())
            .build();
    assertReturnsJob(filter);
  }

  @Test
  public void shoulStopJobById() {
    CoreServiceProto.StopIngestionJobRequest request =
        CoreServiceProto.StopIngestionJobRequest.newBuilder().setId(this.job.getId()).build();

    stub.stopIngestionJob(request);

    assertThat(this.job.getStatus(), equalTo(JobStatus.ABORTING));
    assertThat(this.jobManager.getAllJobs(), hasSize(0));
  }

  @Test
  public void shouldNotStopJobInTransition() {
    this.job.setStatus(JobStatus.UNKNOWN);

    CoreServiceProto.StopIngestionJobRequest request =
        CoreServiceProto.StopIngestionJobRequest.newBuilder().setId(this.job.getId()).build();

    Assertions.assertThrows(StatusRuntimeException.class, () -> stub.stopIngestionJob(request));
  }

  @Test
  public void shouldNotStopUnknownJob() {
    CoreServiceProto.StopIngestionJobRequest request =
        CoreServiceProto.StopIngestionJobRequest.newBuilder().setId("unknown-id").build();

    Assertions.assertThrows(StatusRuntimeException.class, () -> stub.stopIngestionJob(request));
  }

  @Test
  public void shouldRestartJob() {
    CoreServiceProto.RestartIngestionJobRequest request =
        CoreServiceProto.RestartIngestionJobRequest.newBuilder().setId(this.job.getId()).build();
    stub.restartIngestionJob(request);

    assertThat(this.job.getStatus(), equalTo(JobStatus.ABORTING));
  }

  @Test
  public void shouldNotRestartJobInTransition() {
    this.job.setStatus(JobStatus.UNKNOWN);

    CoreServiceProto.RestartIngestionJobRequest request =
        CoreServiceProto.RestartIngestionJobRequest.newBuilder().setId(this.job.getId()).build();

    Assertions.assertThrows(StatusRuntimeException.class, () -> stub.restartIngestionJob(request));
  }

  @TestConfiguration
  public static class TestConfig extends BaseIT.BaseTestConfig {
    @Bean
    public JobManager getJobManager() {
      return new FakeJobManager();
    }
  }
}
