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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

import com.google.protobuf.InvalidProtocolBufferException;
import feast.core.CoreServiceProto.ListIngestionJobsRequest;
import feast.core.CoreServiceProto.ListIngestionJobsResponse;
import feast.core.CoreServiceProto.StopIngestionJobRequest;
import feast.core.CoreServiceProto.StopIngestionJobResponse;
import feast.core.FeatureSetProto.FeatureSetStatus;
import feast.core.FeatureSetReferenceProto.FeatureSetReference;
import feast.core.IngestionJobProto.IngestionJob;
import feast.core.SourceProto.KafkaSourceConfig;
import feast.core.SourceProto.SourceType;
import feast.core.StoreProto.Store.RedisConfig;
import feast.core.StoreProto.Store.StoreType;
import feast.core.dao.FeatureSetRepository;
import feast.core.dao.JobRepository;
import feast.core.job.JobManager;
import feast.core.job.Runner;
import feast.core.model.FeatureSet;
import feast.core.model.Field;
import feast.core.model.Job;
import feast.core.model.JobStatus;
import feast.core.model.Source;
import feast.core.model.Store;
import feast.types.ValueProto.ValueType.Enum;
import java.time.Instant;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

public class JobServiceTest {
  // mocks
  @Mock private FeatureSetRepository featureSetRepository;
  @Mock private JobRepository jobRepository;
  @Mock private JobManager jobManager;
  // fake models
  private Source dataSource;
  private Store dataStore;
  private FeatureSet featureSet;
  private Job job;
  private IngestionJob ingestionJob;
  // test target
  public JobService jobService;

  /* unit test setup */
  @Before
  public void setup() {
    initMocks(this);

    // create mock objects for testing
    // fake data source
    this.dataSource =
        new Source(
            SourceType.KAFKA,
            KafkaSourceConfig.newBuilder()
                .setBootstrapServers("kafka:9092")
                .setTopic("my-topic")
                .build(),
            true);
    // fake data store
    this.dataStore =
        new Store(
            "feast-redis",
            StoreType.REDIS.toString(),
            RedisConfig.newBuilder().setPort(6379).build().toByteArray(),
            "*:*:*");

    // fake featureset & job
    this.featureSet = this.newDummyFeatureSet("food", 2, "hunger");
    this.job = this.newDummyJob("job", "kafka-to-redis", JobStatus.PENDING);
    try {
      this.ingestionJob = this.job.toIngestionProto();
    } catch (InvalidProtocolBufferException e) {
      e.printStackTrace();
    }

    // setup mock objects
    this.setupFeatureSetRepository();
    this.setupJobRepository();
    this.setupJobManager();

    // create test target
    this.jobService =
        new JobService(
            this.jobRepository, this.featureSetRepository, Arrays.asList(this.jobManager));
  }

  // setup fake feature set repository
  public void setupFeatureSetRepository() {
    when(this.featureSetRepository.findFeatureSetByNameAndProject_NameAndVersion(
            "food", "hunger", 2))
        .thenReturn(this.featureSet);
    when(this.featureSetRepository.findAllByNameAndProject_Name("food", "hunger"))
        .thenReturn(Arrays.asList(featureSet));
    when(this.featureSetRepository.findAllByNameAndVersion("food", 2))
        .thenReturn(Arrays.asList(featureSet));
  }

  // setup fake job repository
  public void setupJobRepository() {
    when(this.jobRepository.findById(this.job.getId())).thenReturn(Optional.of(this.job));
    when(this.jobRepository.findByStoreName(this.dataStore.getName()))
        .thenReturn(Arrays.asList(this.job));
    when(this.jobRepository.findByFeatureSetIn(Arrays.asList(this.featureSet)))
        .thenReturn(Arrays.asList(this.job));
  }

  // TODO: setup fake job manager
  public void setupJobManager() {
    when(this.jobManager.getRunnerType()).thenReturn(Runner.DATAFLOW);
  }

  // dummy model constructorss
  private FeatureSet newDummyFeatureSet(String name, int version, String project) {
    Field feature = new Field(name + "_feature", Enum.INT64);
    Field entity = new Field(name + "_entity", Enum.STRING);

    FeatureSet fs =
        new FeatureSet(
            name,
            project,
            version,
            100L,
            Arrays.asList(entity),
            Arrays.asList(feature),
            this.dataSource,
            FeatureSetStatus.STATUS_READY);
    fs.setCreated(Date.from(Instant.ofEpochSecond(10L)));
    return fs;
  }

  private Job newDummyJob(String id, String name, JobStatus status) {
    return new Job(
        id,
        name,
        Runner.DATAFLOW.getName(),
        this.dataSource,
        this.dataStore,
        Arrays.asList(this.featureSet),
        status);
  }

  /* unit tests */
  private ListIngestionJobsResponse tryListJobs(ListIngestionJobsRequest request) {
    ListIngestionJobsResponse response = null;
    try {
      response = this.jobService.listJobs(request);
    } catch (InvalidProtocolBufferException e) {
      e.printStackTrace();
      fail("Caught Unexpected exception");
    }

    return response;
  }

  // list jobs
  @Test
  public void testListJobsById() {
    ListIngestionJobsRequest.Filter filter =
        ListIngestionJobsRequest.Filter.newBuilder().setId(this.job.getId()).build();
    ListIngestionJobsRequest request =
        ListIngestionJobsRequest.newBuilder().setFilter(filter).build();
    assertEquals(this.tryListJobs(request).getJobs(0), this.ingestionJob);
  }

  @Test
  public void testListJobsByStoreName() {
    ListIngestionJobsRequest.Filter filter =
        ListIngestionJobsRequest.Filter.newBuilder().setStoreName(this.dataStore.getName()).build();
    ListIngestionJobsRequest request =
        ListIngestionJobsRequest.newBuilder().setFilter(filter).build();
    assertEquals(this.tryListJobs(request).getJobs(0), this.ingestionJob);
  }

  @Test
  public void testListIngestionJobByFeatureSetReference() {
    // list job by feature set reference: name and version and project
    FeatureSetReference fsReference =
        FeatureSetReference.newBuilder()
            .setVersion(this.featureSet.getVersion())
            .setName(this.featureSet.getName())
            .setProject(this.featureSet.getProject().toString())
            .build();
    ListIngestionJobsRequest.Filter filter =
        ListIngestionJobsRequest.Filter.newBuilder().setId(this.job.getId()).build();
    ListIngestionJobsRequest request =
        ListIngestionJobsRequest.newBuilder().setFilter(filter).build();
    assertEquals(this.tryListJobs(request).getJobs(0), this.ingestionJob);

    // list job by feature set reference: name and version
    fsReference =
        FeatureSetReference.newBuilder()
            .setName(this.featureSet.getName())
            .setProject(this.featureSet.getProject().toString())
            .build();
    filter = ListIngestionJobsRequest.Filter.newBuilder().setId(this.job.getId()).build();
    request = ListIngestionJobsRequest.newBuilder().setFilter(filter).build();
    assertEquals(this.tryListJobs(request).getJobs(0), this.ingestionJob);

    // list job by feature set reference: name and project
    fsReference =
        FeatureSetReference.newBuilder()
            .setName(this.featureSet.getName())
            .setVersion(this.featureSet.getVersion())
            .build();
    filter = ListIngestionJobsRequest.Filter.newBuilder().setId(this.job.getId()).build();
    request = ListIngestionJobsRequest.newBuilder().setFilter(filter).build();
    assertEquals(this.tryListJobs(request).getJobs(0), this.ingestionJob);
  }

  // stop jobs
  private StopIngestionJobResponse tryStopJob(
      StopIngestionJobRequest request, boolean expectError) {
    StopIngestionJobResponse response = null;
    try {
      response = this.jobService.stopJob(request);
      // expected exception, but none was thrown
      if (expectError) {
        fail("Expected exception, but none was thrown");
      }
    } catch (Exception e) {
      if (expectError != true) {
        // unexpected exception
        e.printStackTrace();
        fail("Caught Unexpected exception");
      }
    }

    return response;
  }

  @Test
  public void testStopJobForId() {
    JobStatus prevStatus = this.job.getStatus();
    this.job.setStatus(JobStatus.RUNNING);

    StopIngestionJobRequest request =
        StopIngestionJobRequest.newBuilder().setId(this.job.getId()).build();
    this.tryStopJob(request, false);
    verify(this.jobManager).abortJob(this.job.getExtId());

    this.job.setStatus(prevStatus);
  }

  @Test
  public void testStopAlreadyStop() {
    // check that stop jobs does not trying to stop jobs that are
    // not already stopped/stopping
    List<JobStatus> doNothingStatuses =
        Arrays.asList(
            JobStatus.SUSPENDED, JobStatus.SUSPENDING,
            JobStatus.ABORTED, JobStatus.ABORTING,
            JobStatus.COMPLETED, JobStatus.ABORTED);

    JobStatus prevStatus = this.job.getStatus();
    for (JobStatus status : doNothingStatuses) {
      this.job.setStatus(status);

      StopIngestionJobRequest request =
          StopIngestionJobRequest.newBuilder().setId(this.job.getId()).build();
      this.tryStopJob(request, false);

      verify(this.jobManager, never()).abortJob(this.job.getExtId());
    }

    this.job.setStatus(prevStatus);
  }

  @Test
  public void testStopUnknownJobError() {
    // check for UnsupportedOperationException when trying to stop jobs are
    // in an in unknown state
    JobStatus prevStatus = this.job.getStatus();
    this.job.setStatus(JobStatus.UNKNOWN);

    StopIngestionJobRequest request =
        StopIngestionJobRequest.newBuilder().setId(this.job.getId()).build();
    this.tryStopJob(request, true);

    this.job.setStatus(prevStatus);
  }
}
