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
import static org.junit.Assert.fail;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

import com.google.protobuf.InvalidProtocolBufferException;
import feast.core.CoreServiceProto.ListFeatureSetsRequest;
import feast.core.CoreServiceProto.ListFeatureSetsResponse;
import feast.core.CoreServiceProto.ListIngestionJobsRequest;
import feast.core.CoreServiceProto.ListIngestionJobsResponse;
import feast.core.CoreServiceProto.RestartIngestionJobRequest;
import feast.core.CoreServiceProto.RestartIngestionJobResponse;
import feast.core.CoreServiceProto.StopIngestionJobRequest;
import feast.core.CoreServiceProto.StopIngestionJobResponse;
import feast.core.FeatureSetReferenceProto.FeatureSetReference;
import feast.core.IngestionJobProto.IngestionJob;
import feast.core.StoreProto.Store.RedisConfig;
import feast.core.StoreProto.Store.StoreType;
import feast.core.dao.JobRepository;
import feast.core.job.JobManager;
import feast.core.job.Runner;
import feast.core.model.*;
import feast.types.ValueProto.ValueType.Enum;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

public class JobServiceTest {
  // mocks
  @Mock private JobRepository jobRepository;
  @Mock private JobManager jobManager;
  @Mock private SpecService specService;
  // fake models
  private Source dataSource;
  private Store dataStore;
  private FeatureSet featureSet;
  private List<FeatureSetReference> fsReferences;
  private List<ListFeatureSetsRequest.Filter> listFilters;
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
    this.dataSource = TestObjectFactory.defaultSource;
    // fake data store
    this.dataStore =
        new Store(
            "feast-redis",
            StoreType.REDIS.toString(),
            RedisConfig.newBuilder().setPort(6379).build().toByteArray(),
            "*:*:*");

    // fake featureset & job
    this.featureSet = this.newDummyFeatureSet("food", 2, "hunger");
    this.job = this.newDummyJob("kafka-to-redis", "job-1111", JobStatus.PENDING);
    try {
      this.ingestionJob = this.job.toProto();
    } catch (InvalidProtocolBufferException e) {
      e.printStackTrace();
    }

    this.fsReferences = this.newDummyFeatureSetReferences();
    this.listFilters = this.newDummyListRequestFilters();

    // setup mock objects
    this.setupSpecService();
    this.setupJobRepository();
    this.setupJobManager();

    // create test target
    this.jobService =
        new JobService(this.jobRepository, this.specService, Arrays.asList(this.jobManager));
  }

  // setup fake spec service
  public void setupSpecService() {
    try {
      ListFeatureSetsResponse response =
          ListFeatureSetsResponse.newBuilder().addFeatureSets(this.featureSet.toProto()).build();

      when(this.specService.listFeatureSets(this.listFilters.get(0))).thenReturn(response);

      when(this.specService.listFeatureSets(this.listFilters.get(1))).thenReturn(response);

      when(this.specService.listFeatureSets(this.listFilters.get(2))).thenReturn(response);
    } catch (InvalidProtocolBufferException e) {
      e.printStackTrace();
      fail("Unexpected exception");
    }
  }

  // setup fake job repository
  public void setupJobRepository() {
    when(this.jobRepository.findById(this.job.getId())).thenReturn(Optional.of(this.job));
    when(this.jobRepository.findByStoreName(this.dataStore.getName()))
        .thenReturn(Arrays.asList(this.job));
    when(this.jobRepository.findByFeatureSetsIn(Arrays.asList(this.featureSet)))
        .thenReturn(Arrays.asList(this.job));
    when(this.jobRepository.findAll()).thenReturn(Arrays.asList(this.job));
  }

  // TODO: setup fake job manager
  public void setupJobManager() {
    when(this.jobManager.getRunnerType()).thenReturn(Runner.DATAFLOW);
    when(this.jobManager.restartJob(this.job))
        .thenReturn(this.newDummyJob(this.job.getId(), this.job.getExtId(), JobStatus.PENDING));
  }

  // dummy model constructorss
  private FeatureSet newDummyFeatureSet(String name, int version, String project) {
    Feature feature = TestObjectFactory.CreateFeature(name + "_feature", Enum.INT64);
    Entity entity = TestObjectFactory.CreateEntity(name + "_entity", Enum.STRING);

    FeatureSet fs =
        TestObjectFactory.CreateFeatureSet(
            name, project, Arrays.asList(entity), Arrays.asList(feature));
    fs.setCreated(Date.from(Instant.ofEpochSecond(10L)));
    return fs;
  }

  private Job newDummyJob(String id, String extId, JobStatus status) {
    return new Job(
        id,
        extId,
        Runner.DATAFLOW,
        this.dataSource,
        this.dataStore,
        Arrays.asList(this.featureSet),
        status);
  }

  private List<FeatureSetReference> newDummyFeatureSetReferences() {
    return Arrays.asList(
        // all provided: name, version and project
        FeatureSetReference.newBuilder()
            .setName(this.featureSet.getName())
            .setProject(this.featureSet.getProject().toString())
            .build(),

        // name and project
        FeatureSetReference.newBuilder()
            .setName(this.featureSet.getName())
            .setProject(this.featureSet.getProject().toString())
            .build(),

        // name and version
        FeatureSetReference.newBuilder().setName(this.featureSet.getName()).build());
  }

  private List<ListFeatureSetsRequest.Filter> newDummyListRequestFilters() {
    return Arrays.asList(
        // all provided: name, version and project
        ListFeatureSetsRequest.Filter.newBuilder()
            .setFeatureSetName(this.featureSet.getName())
            .setProject(this.featureSet.getProject().toString())
            .build(),

        // name and project
        ListFeatureSetsRequest.Filter.newBuilder()
            .setFeatureSetName(this.featureSet.getName())
            .setProject(this.featureSet.getProject().toString())
            .build(),

        // name and project
        ListFeatureSetsRequest.Filter.newBuilder()
            .setFeatureSetName(this.featureSet.getName())
            .setProject("*")
            .build());
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
    assertThat(this.tryListJobs(request).getJobs(0), equalTo(this.ingestionJob));

    // list with no filter
    request = ListIngestionJobsRequest.newBuilder().build();
    assertThat(this.tryListJobs(request).getJobs(0), equalTo(this.ingestionJob));

    // list with empty filter
    filter = ListIngestionJobsRequest.Filter.newBuilder().build();
    request = ListIngestionJobsRequest.newBuilder().setFilter(filter).build();
    assertThat(this.tryListJobs(request).getJobs(0), equalTo(this.ingestionJob));
  }

  @Test
  public void testListJobsByStoreName() {
    ListIngestionJobsRequest.Filter filter =
        ListIngestionJobsRequest.Filter.newBuilder().setStoreName(this.dataStore.getName()).build();
    ListIngestionJobsRequest request =
        ListIngestionJobsRequest.newBuilder().setFilter(filter).build();
    assertThat(this.tryListJobs(request).getJobs(0), equalTo(this.ingestionJob));
  }

  @Test
  public void testListIngestionJobByFeatureSetReference() {
    // list job by feature set reference: name and version and project
    ListIngestionJobsRequest.Filter filter =
        ListIngestionJobsRequest.Filter.newBuilder()
            .setFeatureSetReference(this.fsReferences.get(0))
            .setId(this.job.getId())
            .build();
    ListIngestionJobsRequest request =
        ListIngestionJobsRequest.newBuilder().setFilter(filter).build();
    assertThat(this.tryListJobs(request).getJobs(0), equalTo(this.ingestionJob));

    // list job by feature set reference: name and version
    filter =
        ListIngestionJobsRequest.Filter.newBuilder()
            .setFeatureSetReference(this.fsReferences.get(1))
            .setId(this.job.getId())
            .build();
    request = ListIngestionJobsRequest.newBuilder().setFilter(filter).build();
    assertThat(this.tryListJobs(request).getJobs(0), equalTo(this.ingestionJob));

    // list job by feature set reference: name and project
    filter =
        ListIngestionJobsRequest.Filter.newBuilder()
            .setFeatureSetReference(this.fsReferences.get(2))
            .setId(this.job.getId())
            .build();
    request = ListIngestionJobsRequest.newBuilder().setFilter(filter).build();
    assertThat(this.tryListJobs(request).getJobs(0), equalTo(this.ingestionJob));
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
        fail("Caught Unexpected exception trying to restart job");
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

    // TODO: check that for job status change in featureset source

    this.job.setStatus(prevStatus);
  }

  @Test
  public void testStopAlreadyStopped() {
    // check that stop jobs does not trying to stop jobs that are not already stopped
    List<JobStatus> doNothingStatuses = new ArrayList<>(JobStatus.getTerminalStates());

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
  public void testStopUnsupportedError() {
    // check for UnsupportedOperationException when trying to stop jobs are
    // in an in unknown or in a transitional state
    JobStatus prevStatus = this.job.getStatus();
    List<JobStatus> unsupportedStatuses = new ArrayList<>();
    unsupportedStatuses.addAll(JobStatus.getTransitionalStates());
    unsupportedStatuses.add(JobStatus.UNKNOWN);

    for (JobStatus status : unsupportedStatuses) {
      this.job.setStatus(status);

      StopIngestionJobRequest request =
          StopIngestionJobRequest.newBuilder().setId(this.job.getId()).build();
      this.tryStopJob(request, true);
    }

    this.job.setStatus(prevStatus);
  }

  // restart jobs
  private RestartIngestionJobResponse tryRestartJob(
      RestartIngestionJobRequest request, boolean expectError) {
    RestartIngestionJobResponse response = null;
    try {
      response = this.jobService.restartJob(request);
      // expected exception, but none was thrown
      if (expectError) {
        fail("Expected exception, but none was thrown");
      }
    } catch (Exception e) {
      if (expectError != true) {
        // unexpected exception
        e.printStackTrace();
        fail("Caught Unexpected exception trying to stop job");
      }
    }

    return response;
  }

  @Test
  public void testRestartJobForId() {
    JobStatus prevStatus = this.job.getStatus();

    // restart running job
    this.job.setStatus(JobStatus.RUNNING);
    RestartIngestionJobRequest request =
        RestartIngestionJobRequest.newBuilder().setId(this.job.getId()).build();
    this.tryRestartJob(request, false);

    // restart terminated job
    this.job.setStatus(JobStatus.SUSPENDED);
    request = RestartIngestionJobRequest.newBuilder().setId(this.job.getId()).build();
    this.tryRestartJob(request, false);

    verify(this.jobManager, times(2)).restartJob(this.job);
    verify(this.jobRepository, times(2)).saveAndFlush(this.job);

    this.job.setStatus(prevStatus);
  }

  @Test
  public void testRestartUnsupportedError() {
    // check for UnsupportedOperationException when trying to restart jobs are
    // in an in unknown or in a transitional state
    JobStatus prevStatus = this.job.getStatus();
    List<JobStatus> unsupportedStatuses = new ArrayList<>();
    unsupportedStatuses.addAll(JobStatus.getTransitionalStates());
    unsupportedStatuses.add(JobStatus.UNKNOWN);

    for (JobStatus status : unsupportedStatuses) {
      this.job.setStatus(status);

      RestartIngestionJobRequest request =
          RestartIngestionJobRequest.newBuilder().setId(this.job.getId()).build();
      this.tryRestartJob(request, true);
    }

    this.job.setStatus(prevStatus);
  }
}
