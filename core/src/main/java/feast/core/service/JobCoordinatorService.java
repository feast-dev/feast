/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright 2018-2019 The Feast Authors
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

import com.google.protobuf.InvalidProtocolBufferException;
import feast.core.CoreServiceProto.ListFeatureSetsRequest;
import feast.core.CoreServiceProto.ListStoresRequest.Filter;
import feast.core.CoreServiceProto.ListStoresResponse;
import feast.core.FeatureSetProto;
import feast.core.FeatureSetProto.FeatureSetStatus;
import feast.core.StoreProto;
import feast.core.StoreProto.Store.Subscription;
import feast.core.config.FeastProperties;
import feast.core.config.FeastProperties.JobProperties;
import feast.core.dao.FeatureSetRepository;
import feast.core.dao.JobRepository;
import feast.core.job.JobManager;
import feast.core.job.JobUpdateTask;
import feast.core.model.FeatureSet;
import feast.core.model.Job;
import feast.core.model.JobStatus;
import feast.core.model.Source;
import feast.core.model.Store;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Slf4j
@Service
public class JobCoordinatorService {

  private JobRepository jobRepository;
  private FeatureSetRepository featureSetRepository;
  private SpecService specService;
  private JobManager jobManager;
  private JobProperties jobProperties;

  @Autowired
  public JobCoordinatorService(
      JobRepository jobRepository,
      FeatureSetRepository featureSetRepository,
      SpecService specService,
      JobManager jobManager,
      FeastProperties feastProperties) {
    this.jobRepository = jobRepository;
    this.featureSetRepository = featureSetRepository;
    this.specService = specService;
    this.jobManager = jobManager;
    this.jobProperties = feastProperties.getJobs();
  }

  /**
   * Poll does the following:
   *
   * <p>1) Checks DB and extracts jobs that have to run based on the specs available
   *
   * <p>2) Does a diff with the current set of jobs, starts/updates job(s) if necessary
   *
   * <p>3) Updates job object in DB with status, feature sets
   *
   * <p>4) Updates Feature set statuses
   */
  @Transactional
  @Scheduled(fixedDelayString = "${feast.jobs.polling_interval_milliseconds}")
  public void Poll() throws InvalidProtocolBufferException {
    log.info("Polling for new jobs...");
    List<JobUpdateTask> jobUpdateTasks = new ArrayList<>();
    ListStoresResponse listStoresResponse = specService.listStores(Filter.newBuilder().build());
    for (StoreProto.Store store : listStoresResponse.getStoreList()) {
      Set<FeatureSetProto.FeatureSet> featureSets = new HashSet<>();
      for (Subscription subscription : store.getSubscriptionsList()) {
        featureSets.addAll(
            new ArrayList<>(
                specService
                    .listFeatureSets(
                        ListFeatureSetsRequest.Filter.newBuilder()
                            .setFeatureSetName(subscription.getName())
                            .setFeatureSetVersion(subscription.getVersion())
                            .setProject(subscription.getProject())
                            .build())
                    .getFeatureSetsList()));
      }
      if (!featureSets.isEmpty()) {
        featureSets.stream()
            .collect(Collectors.groupingBy(fs -> fs.getSpec().getSource()))
            .entrySet()
            .stream()
            .forEach(
                kv -> {
                  Optional<Job> originalJob =
                      getJob(Source.fromProto(kv.getKey()), Store.fromProto(store));
                  jobUpdateTasks.add(
                      new JobUpdateTask(
                          kv.getValue(),
                          kv.getKey(),
                          store,
                          originalJob,
                          jobManager,
                          jobProperties.getJobUpdateTimeoutSeconds()));
                });
      }
    }
    if (jobUpdateTasks.size() == 0) {
      log.info("No jobs found.");
      return;
    }

    log.info("Creating/Updating {} jobs...", jobUpdateTasks.size());
    ExecutorService executorService = Executors.newFixedThreadPool(jobUpdateTasks.size());
    ExecutorCompletionService<Job> ecs = new ExecutorCompletionService<>(executorService);
    jobUpdateTasks.forEach(ecs::submit);

    int completedTasks = 0;
    while (completedTasks < jobUpdateTasks.size()) {
      try {
        Job job = ecs.take().get();
        if (job != null) {
          jobRepository.saveAndFlush(job);
        }
      } catch (ExecutionException | InterruptedException e) {
        log.warn("Unable to start or update job: {}", e.getMessage());
      }
      completedTasks++;
    }

    log.info("Updating feature set status");
    updateFeatureSetStatuses(jobUpdateTasks);
  }

  // TODO: make this more efficient
  private void updateFeatureSetStatuses(List<JobUpdateTask> jobUpdateTasks) {
    Set<FeatureSet> ready = new HashSet<>();
    Set<FeatureSet> pending = new HashSet<>();
    for (JobUpdateTask jobUpdateTask : jobUpdateTasks) {
      Optional<Job> job =
          getJob(
              Source.fromProto(jobUpdateTask.getSourceSpec()),
              Store.fromProto(jobUpdateTask.getStore()));
      if (job.isPresent()) {
        if (job.get().getStatus() == JobStatus.RUNNING) {
          ready.addAll(job.get().getFeatureSets());
        } else {
          pending.addAll(job.get().getFeatureSets());
        }
      }
    }
    ready.removeAll(pending);
    ready.forEach(
        fs -> {
          fs.setStatus(FeatureSetStatus.STATUS_READY.toString());
          featureSetRepository.save(fs);
        });
    pending.forEach(
        fs -> {
          fs.setStatus(FeatureSetStatus.STATUS_PENDING.toString());
          featureSetRepository.save(fs);
        });
    featureSetRepository.flush();
  }

  @Transactional
  public Optional<Job> getJob(Source source, Store store) {
    List<Job> jobs =
        jobRepository.findBySourceIdAndStoreNameOrderByLastUpdatedDesc(
            source.getId(), store.getName());
    jobs = jobs.stream().filter(job -> !job.hasTerminated()).collect(Collectors.toList());
    if (jobs.isEmpty()) {
      return Optional.empty();
    }
    // return the latest
    return Optional.of(jobs.get(0));
  }
}
