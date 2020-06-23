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

import static feast.core.model.FeatureSet.parseReference;

import com.google.common.collect.Sets;
import com.google.protobuf.InvalidProtocolBufferException;
import feast.core.config.FeastProperties;
import feast.core.config.FeastProperties.JobProperties;
import feast.core.dao.FeatureSetRepository;
import feast.core.dao.JobRepository;
import feast.core.dao.SourceRepository;
import feast.core.job.*;
import feast.core.model.*;
import feast.core.model.FeatureSet;
import feast.core.model.Job;
import feast.core.model.JobStatus;
import feast.core.model.Source;
import feast.core.model.Store;
import feast.proto.core.CoreServiceProto.ListStoresRequest.Filter;
import feast.proto.core.CoreServiceProto.ListStoresResponse;
import feast.proto.core.FeatureSetProto;
import feast.proto.core.IngestionJobProto;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Slf4j
@Service
public class JobCoordinatorService {

  private final int SPEC_PUBLISHING_TIMEOUT_SECONDS = 5;

  private final JobRepository jobRepository;
  private final FeatureSetRepository featureSetRepository;
  private final SourceRepository sourceRepository;
  private final SpecService specService;
  private final JobManager jobManager;
  private final JobProperties jobProperties;
  private final KafkaTemplate<String, FeatureSetProto.FeatureSetSpec> specPublisher;

  @Autowired
  public JobCoordinatorService(
      JobRepository jobRepository,
      FeatureSetRepository featureSetRepository,
      SourceRepository sourceRepository,
      SpecService specService,
      JobManager jobManager,
      FeastProperties feastProperties,
      KafkaTemplate<String, FeatureSetProto.FeatureSetSpec> specPublisher) {
    this.jobRepository = jobRepository;
    this.featureSetRepository = featureSetRepository;
    this.sourceRepository = sourceRepository;
    this.specService = specService;
    this.jobManager = jobManager;
    this.jobProperties = feastProperties.getJobs();
    this.specPublisher = specPublisher;
  }

  /**
   * Poll does the following:
   *
   * <p>1) Checks DB and extracts jobs that have to run based on the specs available
   *
   * <p>2) Does a diff with the current set of jobs, starts/updates/stops job(s) if necessary
   *
   * <p>3) Updates job object in DB with status, feature sets
   *
   * <p>4) Updates Feature set statuses
   */
  @Transactional
  @Scheduled(fixedDelayString = "${feast.jobs.polling_interval_milliseconds}")
  public void Poll() throws InvalidProtocolBufferException {
    log.info("Polling for new jobs...");
    Map<Source, Set<Store>> sourceStoreMappings = getSourceToStoreMappings();
    List<JobTask> jobUpdateTasks = makeJobUpdateTasks(sourceStoreMappings);

    if (jobUpdateTasks.isEmpty()) {
      log.info("No jobs found.");
      return;
    }

    log.info("Creating/Updating {} jobs...", jobUpdateTasks.size());
    startOrUpdateJobs(jobUpdateTasks);
  }

  void startOrUpdateJobs(List<JobTask> tasks) {
    ExecutorService executorService = Executors.newFixedThreadPool(tasks.size());
    ExecutorCompletionService<Job> ecs = new ExecutorCompletionService<>(executorService);
    tasks.forEach(ecs::submit);

    int completedTasks = 0;
    List<Job> startedJobs = new ArrayList<>();
    while (completedTasks < tasks.size()) {
      try {
        Job job = ecs.take().get(jobProperties.getJobUpdateTimeoutSeconds(), TimeUnit.SECONDS);
        if (job != null) {
          startedJobs.add(job);
        }
      } catch (ExecutionException | InterruptedException | TimeoutException e) {
        log.warn("Unable to start or update job: {}", e.getMessage());
      }
      completedTasks++;
    }
    jobRepository.saveAll(startedJobs);
    executorService.shutdown();
  }

  /**
   * Makes Job Update Tasks required to reconcile the current ingestion jobs with the given source
   * to store map. Compares the current ingestion jobs and source to store mapping to determine
   * which whether jobs have started/stopped/updated in terms of Job Update tasks. Only tries to
   * stop ingestion jobs its the required ingestions to maintained ingestion jobs are already
   * RUNNING.
   *
   * @param sourceStoresMappings a list of source to store pairs where ingestion jobs would have to
   *     be maintained for ingestion to work correctly.
   * @return list of job update tasks required to reconcile the current ingestion jobs to the state
   *     that is defined by sourceStoreMap.
   */
  List<JobTask> makeJobUpdateTasks(Map<Source, Set<Store>> sourceStoresMappings) {
    List<JobTask> jobTasks = new LinkedList<>();
    // Ensure a running job for each source to store mapping
    List<Job> activeJobs = new LinkedList<>();
    boolean isSafeToStopJobs = true;

    for (Map.Entry<Source, Set<Store>> mapping : sourceStoresMappings.entrySet()) {
      Source source = mapping.getKey();
      Set<Store> stores = mapping.getValue();
      Set<FeatureSet> featureSets =
          stores.stream()
              .flatMap(s -> getFeatureSetsForStore(s).stream())
              .collect(Collectors.toSet());

      Job job = getOrCreateJob(source, stores);

      if (job.isDeployed()) {
        if (!job.isRunning()) {
          jobTasks.add(UpdateJobStatusTask.builder().setJob(job).setJobManager(jobManager).build());

          // Mark that it is not safe to stop jobs without disrupting ingestion
          isSafeToStopJobs = false;
          continue;
        }

        if (jobRequiresUpgrade(job, stores)) {
          job.setStores(stores);

          jobTasks.add(UpgradeJobTask.builder().setJob(job).setJobManager(jobManager).build());
        } else {
          jobTasks.add(UpdateJobStatusTask.builder().setJob(job).setJobManager(jobManager).build());
        }
      } else {
        jobTasks.add(CreateJobTask.builder().setJob(job).setJobManager(jobManager).build());
      }

      allocateFeatureSets(job, featureSets);

      // Record the job as required to safeguard it from getting stopped
      activeJobs.add(job);
    }
    // Stop extra jobs that are not required to maintain ingestion when safe
    if (isSafeToStopJobs) {
      getExtraJobs(activeJobs)
          .forEach(
              extraJob -> {
                jobTasks.add(
                    TerminateJobTask.builder().setJob(extraJob).setJobManager(jobManager).build());
              });
    }

    return jobTasks;
  }

  /**
   * Decides whether we need to upgrade (restart) given job. Since we send updated FeatureSets to
   * IngestionJob via Kafka, and there's only one source per job (if it change - new job would be
   * created) the only things that can cause upgrade here are stores: new stores can be added, or
   * existing stores will change subscriptions.
   *
   * @param job {@link Job} to check
   * @param stores Set of {@link Store} new version of stores (vs current version job.getStores())
   * @return boolean - need to upgrade
   */
  private boolean jobRequiresUpgrade(Job job, Set<Store> stores) {
    // if store subscriptions have changed
    if (!Sets.newHashSet(stores).equals(Sets.newHashSet(job.getStores()))) {
      return true;
    }

    return false;
  }

  /**
   * Connects given {@link Job} with FeatureSets by creating {@link FeatureSetJobStatus}. This
   * connection represents responsibility of the job to handle allocated FeatureSets. We use this
   * connection {@link FeatureSetJobStatus} to monitor Ingestion of specific FeatureSet and Specs
   * delivery status.
   *
   * <p>Only after this connection is created FeatureSetSpec could be sent to IngestionJob.
   *
   * @param job {@link Job} responsible job
   * @param featureSets Set of {@link FeatureSet} featureSets to allocate to this job
   */
  void allocateFeatureSets(Job job, Set<FeatureSet> featureSets) {
    Map<FeatureSet, FeatureSetJobStatus> alreadyConnected =
        job.getFeatureSetJobStatuses().stream()
            .collect(Collectors.toMap(FeatureSetJobStatus::getFeatureSet, s -> s));

    for (FeatureSet fs : featureSets) {
      if (alreadyConnected.containsKey(fs)) {
        continue;
      }

      FeatureSetJobStatus status = new FeatureSetJobStatus();
      status.setFeatureSet(fs);
      status.setJob(job);
      if (fs.getStatus() == FeatureSetProto.FeatureSetStatus.STATUS_READY) {
        // Feature Set was already delivered to previous generation of the job
        // (another words, it exists in kafka)
        // so we expect Job will ack latest version based on history from kafka topic
        status.setVersion(fs.getVersion());
      }
      status.setDeliveryStatus(FeatureSetProto.FeatureSetJobDeliveryStatus.STATUS_IN_PROGRESS);
      job.getFeatureSetJobStatuses().add(status);
    }
  }

  /** Get the non terminated ingestion job ingesting for given source. */
  public Job getOrCreateJob(Source source, Set<Store> stores) {
    return jobRepository
        .findFirstBySourceTypeAndSourceConfigAndStoreNameAndStatusNotInOrderByLastUpdatedDesc(
            source.getType(), source.getConfig(), null, JobStatus.getTerminalStates())
        .orElseGet(
            () ->
                Job.builder()
                    .setRunner(jobManager.getRunnerType())
                    .setSource(source)
                    .setStores(stores)
                    .setFeatureSetJobStatuses(new HashSet<>())
                    .build());
  }

  /** Get running extra ingestion jobs that have ids not in keepJobs */
  @Transactional
  private Collection<Job> getExtraJobs(List<Job> keepJobs) {
    List<Job> runningJobs = jobRepository.findByStatus(JobStatus.RUNNING);
    Map<String, Job> extraJobMap =
        runningJobs.stream().collect(Collectors.toMap(job -> job.getId(), job -> job));
    keepJobs.forEach(job -> extraJobMap.remove(job.getId()));
    return extraJobMap.values();
  }

  /**
   * Generate a source to stores mapping. The mapping maps the source to Set-of-stores in which
   * ingestion jobs would have to be maintained for ingestion to work correctly.
   *
   * @return a Map from source to stores.
   */
  private Map<Source, Set<Store>> getSourceToStoreMappings() {
    ListStoresResponse listStoresResponse = specService.listStores(Filter.newBuilder().build());
    List<Store> stores =
        listStoresResponse.getStoreList().stream()
            .map(Store::fromProto)
            .collect(Collectors.toList());

    // build mapping from source to store.
    // compile a set of sources via subscribed FeatureSets of stores.

    return stores.stream()
        .flatMap(
            store ->
                getFeatureSetsForStore(store).stream()
                    .map(FeatureSet::getSource)
                    .map(source -> Pair.of(source, store)))
        .distinct()
        .collect(
            Collectors.groupingBy(
                Pair::getLeft, Collectors.mapping(Pair::getRight, Collectors.toSet())));
  }

  /**
   * Get the FeatureSets that the given store subscribes to.
   *
   * @param store to get subscribed FeatureSets for
   * @return list of FeatureSets that the store subscribes to.
   */
  @Transactional
  private List<FeatureSet> getFeatureSetsForStore(Store store) {
    return store.getSubscriptions().stream()
        .flatMap(
            subscription ->
                featureSetRepository
                    .findAllByNameLikeAndProject_NameLikeOrderByNameAsc(
                        subscription.getName().replace('*', '%'),
                        subscription.getProject().replace('*', '%'))
                    .stream())
        .distinct()
        .collect(Collectors.toList());
  }

  @Transactional
  @Scheduled(fixedDelayString = "${feast.stream.specsOptions.notifyIntervalMilliseconds}")
  public void notifyJobsWhenFeatureSetUpdated() {
    List<FeatureSet> pendingFeatureSets =
        featureSetRepository.findAllByStatus(FeatureSetProto.FeatureSetStatus.STATUS_PENDING);

    pendingFeatureSets.stream()
        .filter(
            fs -> {
              List<FeatureSetJobStatus> runningJobs =
                  fs.getJobStatuses().stream()
                      .filter(jobStatus -> jobStatus.getJob().isRunning())
                      .collect(Collectors.toList());

              return runningJobs.size() > 0
                  && runningJobs.stream()
                      .allMatch(jobStatus -> jobStatus.getVersion() < fs.getVersion());
            })
        .forEach(
            fs -> {
              log.info("Sending new FeatureSet {} to Ingestion", fs.getReference());

              // Sending latest version of FeatureSet to all currently running IngestionJobs
              // (there's one topic for all sets).
              // All related jobs would apply new FeatureSet on the fly.
              // In case kafka doesn't respond within SPEC_PUBLISHING_TIMEOUT_SECONDS we will try
              // again later.
              try {
                specPublisher
                    .sendDefault(fs.getReference(), fs.toProto().getSpec())
                    .get(SPEC_PUBLISHING_TIMEOUT_SECONDS, TimeUnit.SECONDS);
              } catch (Exception e) {
                log.error(
                    "Error occurred while sending FeatureSetSpec to kafka. Cause {}."
                        + " Will retry later",
                    e.getMessage());
                return;
              }

              // Updating delivery status for related jobs (that are currently using this
              // FeatureSet).
              // We now set status to IN_PROGRESS, so listenAckFromJobs would be able to
              // monitor delivery progress for each new version.
              fs.getJobStatuses().stream()
                  .filter(s -> s.getJob().isRunning())
                  .forEach(
                      jobStatus -> {
                        jobStatus.setDeliveryStatus(
                            FeatureSetProto.FeatureSetJobDeliveryStatus.STATUS_IN_PROGRESS);
                        jobStatus.setVersion(fs.getVersion());
                      });
              featureSetRepository.saveAndFlush(fs);
            });
  }

  /**
   * Listener for ACK messages coming from IngestionJob when FeatureSetSpec is installed (in
   * pipeline).
   *
   * <p>Updates FeatureSetJobStatus for respected FeatureSet (selected by reference) and Job (select
   * by Id).
   *
   * <p>When all related (running) to FeatureSet jobs are updated - FeatureSet receives READY status
   *
   * @param record ConsumerRecord with key: FeatureSet reference and value: Ack message
   */
  @KafkaListener(topics = {"${feast.stream.specsOptions.specsAckTopic}"})
  @Transactional
  public void listenAckFromJobs(
      ConsumerRecord<String, IngestionJobProto.FeatureSetSpecAck> record) {
    String setReference = record.key();
    Pair<String, String> projectAndSetName = parseReference(setReference);
    FeatureSet featureSet =
        featureSetRepository.findFeatureSetByNameAndProject_Name(
            projectAndSetName.getRight(), projectAndSetName.getLeft());
    if (featureSet == null) {
      log.warn(
          String.format("ACKListener received message for unknown FeatureSet %s", setReference));
      return;
    }

    int ackVersion = record.value().getFeatureSetVersion();

    if (featureSet.getVersion() != ackVersion) {
      log.warn(
          String.format(
              "ACKListener received outdated ack for %s. Current %d, Received %d",
              setReference, featureSet.getVersion(), ackVersion));
      return;
    }

    log.info("Updating featureSet {} delivery statuses.", featureSet.getReference());

    featureSet.getJobStatuses().stream()
        .filter(
            js ->
                js.getJob().getId().equals(record.value().getJobName())
                    && js.getVersion() == ackVersion)
        .findFirst()
        .ifPresent(
            featureSetJobStatus ->
                featureSetJobStatus.setDeliveryStatus(
                    FeatureSetProto.FeatureSetJobDeliveryStatus.STATUS_DELIVERED));

    boolean allDelivered =
        featureSet.getJobStatuses().stream()
            .filter(js -> js.getJob().isRunning())
            .allMatch(
                js ->
                    js.getDeliveryStatus()
                        .equals(FeatureSetProto.FeatureSetJobDeliveryStatus.STATUS_DELIVERED));

    if (allDelivered) {
      log.info("FeatureSet {} update is completely delivered", featureSet.getReference());

      featureSet.setStatus(FeatureSetProto.FeatureSetStatus.STATUS_READY);
      featureSetRepository.saveAndFlush(featureSet);
    }
  }
}
