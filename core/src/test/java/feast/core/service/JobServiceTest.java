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

import static feast.core.util.TestUtil.makeFeatureSetJobStatus;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.protobuf.InvalidProtocolBufferException;
import feast.core.dao.JobRepository;
import feast.core.job.JobManager;
import feast.core.job.Runner;
import feast.core.model.*;
import feast.core.util.TestUtil;
import feast.proto.core.CoreServiceProto.ListFeatureSetsRequest;
import feast.proto.core.CoreServiceProto.ListFeatureSetsResponse;
import feast.proto.core.CoreServiceProto.ListIngestionJobsRequest;
import feast.proto.core.CoreServiceProto.ListIngestionJobsResponse;
import feast.proto.core.CoreServiceProto.RestartIngestionJobRequest;
import feast.proto.core.CoreServiceProto.RestartIngestionJobResponse;
import feast.proto.core.CoreServiceProto.StopIngestionJobRequest;
import feast.proto.core.CoreServiceProto.StopIngestionJobResponse;
import feast.proto.core.FeatureSetReferenceProto.FeatureSetReference;
import feast.proto.core.IngestionJobProto.IngestionJob;
import feast.proto.core.StoreProto.Store.RedisConfig;
import feast.proto.core.StoreProto.Store.StoreType;
import feast.proto.types.ValueProto.ValueType.Enum;
import java.time.Instant;
import java.util.*;
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

  @Before
  public void setup() {
    initMocks(this);

    // create mock objects for testing
    // fake data source
    this.dataSource = TestUtil.defaultSource;
    // fake data store
    this.dataStore =
        new Store(
            "feast-redis",
            StoreType.REDIS.toString(),
            RedisConfig.newBuilder().setPort(6379).build().toByteArray(),
            "*:*:*");

    // fake featureset & job
    this.featureSet = this.newDummyFeatureSet("food", "hunger");
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

  public void setupSpecService() {
    try {
      ListFeatureSetsResponse response =
          ListFeatureSetsResponse.newBuilder().addFeatureSets(this.featureSet.toProto()).build();

      when(this.specService.listFeatureSets(this.listFilters.get(0))).thenReturn(response);

      when(this.specService.listFeatureSets(this.listFilters.get(1))).thenReturn(response);

    } catch (InvalidProtocolBufferException e) {
      e.printStackTrace();
      fail("Unexpected exception");
    }
  }

  public void setupJobRepository() {
    when(this.jobRepository.findById(this.job.getId())).thenReturn(Optional.of(this.job));
    when(this.jobRepository.findByJobStoresIdStoreName(this.dataStore.getName()))
        .thenReturn(Arrays.asList(this.job));
    when(this.jobRepository.findByFeatureSetJobStatusesIn(
            Lists.newArrayList((this.featureSet.getJobStatuses()))))
        .thenReturn(Arrays.asList(this.job));
    when(this.jobRepository.findAll()).thenReturn(Arrays.asList(this.job));
  }

  public void setupJobManager() {
    when(this.jobManager.getRunnerType()).thenReturn(Runner.DATAFLOW);
    when(this.jobManager.restartJob(this.job))
        .thenReturn(this.newDummyJob(this.job.getId(), this.job.getExtId(), JobStatus.PENDING));
  }

  private FeatureSet newDummyFeatureSet(String name, String project) {
    Feature feature = TestUtil.CreateFeature(name + "_feature", Enum.INT64);
    Entity entity = TestUtil.CreateEntity(name + "_entity", Enum.STRING);

    FeatureSet fs =
        TestUtil.CreateFeatureSet(name, project, Arrays.asList(entity), Arrays.asList(feature));
    fs.setCreated(Date.from(Instant.ofEpochSecond(10L)));
    return fs;
  }

  private Job newDummyJob(String id, String extId, JobStatus status) {
    Job job =
        Job.builder()
            .setId(id)
            .setExtId(extId)
            .setRunner(Runner.DATAFLOW)
            .setSource(this.dataSource)
            .setFeatureSetJobStatuses(makeFeatureSetJobStatus(this.featureSet))
            .setStatus(status)
            .build();
    job.setStores(ImmutableSet.of(this.dataStore));
    return job;
  }

  private List<FeatureSetReference> newDummyFeatureSetReferences() {
    return Arrays.asList(
        // name and project
        FeatureSetReference.newBuilder()
            .setName(this.featureSet.getName())
            .setProject(this.featureSet.getProject().toString())
            .build(),

        // name only
        FeatureSetReference.newBuilder().setName(this.featureSet.getName()).build());
  }

  private List<ListFeatureSetsRequest.Filter> newDummyListRequestFilters() {
    return Arrays.asList(
        // name and project
        ListFeatureSetsRequest.Filter.newBuilder()
            .setFeatureSetName(this.featureSet.getName())
            .setProject(this.featureSet.getProject().toString())
            .build(),

        // name  only
        ListFeatureSetsRequest.Filter.newBuilder()
            .setFeatureSetName(this.featureSet.getName())
            .setProject("*")
            .build());
  }

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
    // list job by feature set reference: name and project
    ListIngestionJobsRequest.Filter filter =
        ListIngestionJobsRequest.Filter.newBuilder()
            .setFeatureSetReference(this.fsReferences.get(0))
            .setId(this.job.getId())
            .build();
    ListIngestionJobsRequest request =
        ListIngestionJobsRequest.newBuilder().setFilter(filter).build();
    assertThat(this.tryListJobs(request).getJobs(0), equalTo(this.ingestionJob));

    // list job by feature set reference: name
    filter =
        ListIngestionJobsRequest.Filter.newBuilder()
            .setFeatureSetReference(this.fsReferences.get(1))
            .setId(this.job.getId())
            .build();
    request = ListIngestionJobsRequest.newBuilder().setFilter(filter).build();
    assertThat(this.tryListJobs(request).getJobs(0), equalTo(this.ingestionJob));
  }

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
      if (!expectError) {
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
    when(this.jobManager.abortJob(this.job))
        .thenReturn(this.newDummyJob(job.getId(), job.getExtId(), JobStatus.ABORTING));
    this.tryStopJob(request, false);
    verify(this.jobManager).abortJob(this.job);

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

      verify(this.jobManager, never()).abortJob(this.job);
    }

    this.job.setStatus(prevStatus);
  }

  @Test
  public void testStopUnsupportedError() {
    // check for UnsupportedOperationException when trying to stop jobs are
    // in an in unknown or in a transitional state
    JobStatus prevStatus = this.job.getStatus();
    List<JobStatus> unsupportedStatuses = new ArrayList<>(JobStatus.getTransitionalStates());
    unsupportedStatuses.add(JobStatus.UNKNOWN);

    for (JobStatus status : unsupportedStatuses) {
      this.job.setStatus(status);

      StopIngestionJobRequest request =
          StopIngestionJobRequest.newBuilder().setId(this.job.getId()).build();
      this.tryStopJob(request, true);
    }

    this.job.setStatus(prevStatus);
  }

  @Test(expected = NoSuchElementException.class)
  public void testStopJobForUnknownId() {
    var request = StopIngestionJobRequest.newBuilder().setId("bogusJobId").build();
    jobService.stopJob(request);
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
      if (!expectError) {
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
    List<JobStatus> unsupportedStatuses = new ArrayList<>(JobStatus.getTransitionalStates());
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
