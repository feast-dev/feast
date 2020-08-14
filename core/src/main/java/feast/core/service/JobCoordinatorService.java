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

import static feast.common.models.Store.isSubscribedToFeatureSet;
import static feast.core.model.FeatureSet.parseReference;

import com.google.common.collect.Sets;
import com.google.protobuf.InvalidProtocolBufferException;
import feast.common.models.FeatureSetReference;
import feast.core.config.FeastProperties;
import feast.core.config.FeastProperties.JobProperties;
import feast.core.job.*;
import feast.core.job.task.*;
import feast.core.model.FeatureSetDeliveryStatus;
import feast.core.model.Job;
import feast.core.model.JobStatus;
import feast.proto.core.CoreServiceProto;
import feast.proto.core.CoreServiceProto.ListStoresRequest.Filter;
import feast.proto.core.CoreServiceProto.ListStoresResponse;
import feast.proto.core.FeatureSetProto;
import feast.proto.core.FeatureSetProto.FeatureSet;
import feast.proto.core.FeatureSetProto.FeatureSetSpec;
import feast.proto.core.FeatureSetReferenceProto;
import feast.proto.core.IngestionJobProto;
import feast.proto.core.SourceProto.Source;
import feast.proto.core.StoreProto.Store;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@ConditionalOnProperty("feast.jobs.enabled")
public class JobCoordinatorService {

  private final int SPEC_PUBLISHING_TIMEOUT_SECONDS = 5;

  private final JobRepository jobRepository;
  private final SpecService specService;
  private final JobManager jobManager;
  private final JobProperties jobProperties;
  private final JobGroupingStrategy groupingStrategy;
  private final KafkaTemplate<String, FeatureSetSpec> specPublisher;
  private final List<Store.Subscription> featureSetSubscriptions;
  private final List<String> whitelistedStores;

  @Autowired
  public JobCoordinatorService(
      JobRepository jobRepository,
      SpecService specService,
      JobManager jobManager,
      FeastProperties feastProperties,
      JobGroupingStrategy groupingStrategy,
      KafkaTemplate<String, FeatureSetSpec> specPublisher) {
    this.jobRepository = jobRepository;
    this.specService = specService;
    this.jobManager = jobManager;
    this.jobProperties = feastProperties.getJobs();
    this.specPublisher = specPublisher;
    this.groupingStrategy = groupingStrategy;
    this.featureSetSubscriptions =
        feastProperties.getJobs().getCoordinator().getFeatureSetSelector().stream()
            .map(JobProperties.CoordinatorProperties.FeatureSetSelector::toSubscription)
            .collect(Collectors.toList());
    this.whitelistedStores = feastProperties.getJobs().getCoordinator().getWhitelistedStores();
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
  @Scheduled(fixedDelayString = "${feast.jobs.polling_interval_milliseconds}")
  public void Poll() {
    log.info("Polling for new jobs...");
    Iterable<Pair<Source, Set<Store>>> sourceStoreMappings = getSourceToStoreMappings();
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
    List<Job> processedJobs = new ArrayList<>();
    while (completedTasks < tasks.size()) {
      try {
        Job job = ecs.take().get(jobProperties.getJobUpdateTimeoutSeconds(), TimeUnit.SECONDS);
        if (job != null) {
          processedJobs.add(job);
        }
      } catch (ExecutionException | InterruptedException | TimeoutException e) {
        log.warn("Unable to start or update job: {}", e.getMessage());
        e.printStackTrace();
      }
      completedTasks++;
    }
    processedJobs.forEach(jobRepository::add);
    executorService.shutdown();
  }

  /**
   * Makes Job Update Tasks required to reconcile the current ingestion jobs with the given source
   * to store map. Compares the current ingestion jobs and source to store mapping to determine
   * which whether jobs have started/stopped/updated in terms of Job Update tasks. Only tries to
   * stop ingestion jobs its the required ingestions to maintained ingestion jobs are already
   * RUNNING.
   *
   * @param sourceToStores a iterable of source to stores pairs where ingestion jobs would have to
   *     be maintained for ingestion to work correctly.
   * @return list of job update tasks required to reconcile the current ingestion jobs to the state
   *     that is defined by sourceStoreMap.
   */
  List<JobTask> makeJobUpdateTasks(Iterable<Pair<Source, Set<Store>>> sourceToStores) {
    List<JobTask> jobTasks = new LinkedList<>();
    // Ensure a running job for each source to store mapping
    List<Job> activeJobs = new LinkedList<>();
    boolean isSafeToStopJobs = true;

    for (Pair<Source, Set<Store>> mapping : sourceToStores) {
      Source source = mapping.getKey();
      Set<Store> stores = mapping.getValue();

      Job job = groupingStrategy.getOrCreateJob(source, stores);

      if (job.isDeployed()) {
        if (!job.isRunning()) {
          jobTasks.add(new UpdateJobStatusTask(job, jobManager));

          // Mark that it is not safe to stop jobs without disrupting ingestion
          isSafeToStopJobs = false;
          continue;
        }

        if (jobRequiresUpgrade(job, stores) && job.isRunning()) {
          // Since we want to upgrade job without downtime
          // it would make sense to spawn clone of current job
          // and terminate old version on the next Poll.
          // Both jobs should be in the same consumer group and not conflict with each other
          job = job.cloneWithId(groupingStrategy.createJobId(job));
          job.addAllStores(stores);

          isSafeToStopJobs = false;

          jobTasks.add(new CreateJobTask(job, jobManager));
        } else {
          jobTasks.add(new UpdateJobStatusTask(job, jobManager));
        }
      } else {
        job.addAllFeatureSets(
            stores.stream()
                .flatMap(s -> getFeatureSetsForStore(s).stream())
                .filter(fs -> fs.getSpec().getSource().equals(source))
                .collect(Collectors.toSet()));

        jobTasks.add(new CreateJobTask(job, jobManager));
      }

      // Record the job as required to safeguard it from getting stopped
      activeJobs.add(job);
    }
    // Stop extra jobs that are not required to maintain ingestion when safe
    if (isSafeToStopJobs) {
      getExtraJobs(activeJobs)
          .forEach(
              extraJob -> {
                jobTasks.add(new TerminateJobTask(extraJob, jobManager));
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
    if (!Sets.newHashSet(stores).equals(Sets.newHashSet(job.getStores().values()))) {
      return true;
    }

    return false;
  }

  /**
   * Connects given {@link FeatureSet} with Jobs by creating {@link FeatureSetDeliveryStatus}. This
   * connection represents responsibility of the job to handle allocated FeatureSet. We use this
   * connection {@link FeatureSetDeliveryStatus} to monitor Ingestion of specific FeatureSet and
   * Specs delivery status.
   *
   * <p>Only after this connection is created FeatureSetSpec could be sent to IngestionJob.
   *
   * @param featureSet featureSet {@link FeatureSet} to find jobs and allocate
   */
  FeatureSet allocateFeatureSetToJobs(FeatureSet featureSet) {
    FeatureSetReference ref =
        FeatureSetReference.of(featureSet.getSpec().getProject(), featureSet.getSpec().getName());
    Set<String> confirmedJobIds = new HashSet<>();

    Stream<Pair<Source, Store>> jobArgsStream =
        getAllStores().stream()
            .filter(
                s ->
                    isSubscribedToFeatureSet(
                        s.getSubscriptionsList(),
                        featureSet.getSpec().getProject(),
                        featureSet.getSpec().getName()))
            .map(s -> Pair.of(featureSet.getSpec().getSource(), s));

    // Add featureSet to allocated job if not allocated before
    for (Pair<Source, Set<Store>> jobArgs : groupingStrategy.collectSingleJobInput(jobArgsStream)) {
      Job job = groupingStrategy.getOrCreateJob(jobArgs.getLeft(), jobArgs.getRight());
      if (!job.isRunning()) {
        continue;
      }

      FeatureSetDeliveryStatus status = new FeatureSetDeliveryStatus();
      status.setFeatureSetReference(ref);
      status.setDeliveryStatus(FeatureSetProto.FeatureSetJobDeliveryStatus.STATUS_IN_PROGRESS);
      status.setDeliveredVersion(0);

      Map<FeatureSetReference, FeatureSetDeliveryStatus> deliveryStatuses =
          job.getFeatureSetDeliveryStatuses();

      if (!deliveryStatuses.containsKey(ref)) {
        deliveryStatuses.put(status.getFeatureSetReference(), status);
      }

      confirmedJobIds.add(job.getId());
    }

    // remove from other jobs that was not confirmed
    for (Job job : jobRepository.findByFeatureSetReference(ref)) {
      if (!confirmedJobIds.contains(job.getId())) {
        job.getFeatureSetDeliveryStatuses().remove(ref);
      }
    }
    return featureSet;
  }

  /** Get running extra ingestion jobs that have ids not in keepJobs */
  private Collection<Job> getExtraJobs(List<Job> keepJobs) {
    List<Job> runningJobs = jobRepository.findByStatus(JobStatus.RUNNING);
    Map<String, Job> extraJobMap =
        runningJobs.stream().collect(Collectors.toMap(job -> job.getId(), job -> job));
    keepJobs.forEach(job -> extraJobMap.remove(job.getId()));
    return extraJobMap.values();
  }

  private List<Store> getAllStores() {
    ListStoresResponse listStoresResponse = specService.listStores(Filter.newBuilder().build());
    return listStoresResponse.getStoreList().stream()
        .filter(s -> this.whitelistedStores.contains(s.getName()))
        .collect(Collectors.toList());
  }

  /**
   * Generate a source to stores mapping. The resulting iterable yields pairs of Source and
   * Set-of-stores to create one ingestion job per each pair.
   *
   * @return a Map from source to stores.
   */
  Iterable<Pair<Source, Set<Store>>> getSourceToStoreMappings() {
    // build mapping from source to store.
    // compile a set of sources via subscribed FeatureSets of stores.
    Stream<Pair<Source, Store>> distinctPairs =
        getAllStores().stream()
            .flatMap(
                store ->
                    getFeatureSetsForStore(store).stream()
                        .map(f -> f.getSpec().getSource())
                        .map(source -> Pair.of(source, store)))
            .distinct();
    return groupingStrategy.collectSingleJobInput(distinctPairs);
  }

  /**
   * Get the FeatureSets that the given store subscribes to.
   *
   * @param store to get subscribed FeatureSets for
   * @return list of FeatureSets that the store subscribes to.
   */
  List<FeatureSet> getFeatureSetsForStore(Store store) {
    return store.getSubscriptionsList().stream()
        .flatMap(
            subscription -> {
              try {
                return specService
                    .listFeatureSets(
                        CoreServiceProto.ListFeatureSetsRequest.Filter.newBuilder()
                            .setProject(subscription.getProject())
                            .setFeatureSetName(subscription.getName())
                            .build())
                    .getFeatureSetsList().stream()
                    .filter(
                        f ->
                            this.featureSetSubscriptions.isEmpty()
                                || isSubscribedToFeatureSet(
                                    this.featureSetSubscriptions,
                                    f.getSpec().getProject(),
                                    f.getSpec().getName()));
              } catch (InvalidProtocolBufferException e) {
                throw new RuntimeException(
                    String.format(
                        "Couldn't fetch featureSets for subscription %s. Reason: %s",
                        subscription, e.getMessage()));
              }
            })
        .distinct()
        .collect(Collectors.toList());
  }

  @Scheduled(fixedDelayString = "${feast.stream.specsOptions.notifyIntervalMilliseconds}")
  public void notifyJobsWhenFeatureSetUpdated() throws InvalidProtocolBufferException {
    List<FeatureSet> pendingFeatureSets =
        specService
            .listFeatureSets(
                CoreServiceProto.ListFeatureSetsRequest.Filter.newBuilder()
                    .setProject("*")
                    .setFeatureSetName("*")
                    .setStatus(FeatureSetProto.FeatureSetStatus.STATUS_PENDING)
                    .build())
            .getFeatureSetsList();

    pendingFeatureSets.stream()
        .map(this::allocateFeatureSetToJobs)
        .map(
            fs -> {
              FeatureSetReference ref =
                  FeatureSetReference.of(fs.getSpec().getProject(), fs.getSpec().getName());
              List<FeatureSetDeliveryStatus> deliveryStatuses =
                  jobRepository.findByFeatureSetReference(ref).stream()
                      .filter(Job::isRunning)
                      .flatMap(job -> job.getFeatureSetDeliveryStatuses().values().stream())
                      .filter(jobStatus -> jobStatus.getFeatureSetReference().equals(ref))
                      .collect(Collectors.toList());

              return Pair.of(fs, deliveryStatuses);
            })
        .filter(
            pair ->
                pair.getRight().size() > 0
                    && pair.getRight().stream()
                        .anyMatch(
                            jobStatus ->
                                jobStatus.getDeliveredVersion()
                                    < pair.getLeft().getSpec().getVersion()))
        .forEach(
            pair -> {
              FeatureSet fs = pair.getLeft();
              List<FeatureSetDeliveryStatus> deliveryStatuses = pair.getRight();

              FeatureSetReference ref =
                  FeatureSetReference.of(fs.getSpec().getProject(), fs.getSpec().getName());

              log.info("Sending new FeatureSet {} to Ingestion", ref);

              // Sending latest version of FeatureSet to all currently running IngestionJobs
              // (there's one topic for all sets).
              // All related jobs would apply new FeatureSet on the fly.
              // In case kafka doesn't respond within SPEC_PUBLISHING_TIMEOUT_SECONDS we will try
              // again later.
              try {
                specPublisher
                    .sendDefault(ref.getReference(), fs.getSpec())
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
              deliveryStatuses.forEach(
                  jobStatus -> {
                    jobStatus.setDeliveryStatus(
                        FeatureSetProto.FeatureSetJobDeliveryStatus.STATUS_IN_PROGRESS);
                    jobStatus.setDeliveredVersion(fs.getSpec().getVersion());
                  });
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
  @KafkaListener(
      topics = {"${feast.stream.specsOptions.specsAckTopic}"},
      containerFactory = "kafkaAckListenerContainerFactory")
  public void listenAckFromJobs(ConsumerRecord<String, IngestionJobProto.FeatureSetSpecAck> record)
      throws InvalidProtocolBufferException {
    String setReference = record.key();
    Pair<String, String> projectAndSetName = parseReference(setReference);
    FeatureSet featureSet =
        specService
            .getFeatureSet(
                CoreServiceProto.GetFeatureSetRequest.newBuilder()
                    .setProject(projectAndSetName.getLeft())
                    .setName(projectAndSetName.getRight())
                    .build())
            .getFeatureSet();

    if (featureSet == null) {
      log.warn(
          String.format("ACKListener received message for unknown FeatureSet %s", setReference));
      return;
    }

    int ackVersion = record.value().getFeatureSetVersion();

    if (featureSet.getSpec().getVersion() != ackVersion) {
      log.warn(
          String.format(
              "ACKListener received outdated ack for %s. Current %d, Received %d",
              setReference, featureSet.getSpec().getVersion(), ackVersion));
      return;
    }

    FeatureSetReference ref =
        FeatureSetReference.of(featureSet.getSpec().getProject(), featureSet.getSpec().getName());

    log.info("Updating featureSet {} delivery statuses.", ref);

    jobRepository
        .findById(record.value().getJobName())
        .map(j -> j.getFeatureSetDeliveryStatuses().get(ref))
        .filter(deliveryStatus -> deliveryStatus.getDeliveredVersion() == ackVersion)
        .ifPresent(
            deliveryStatus ->
                deliveryStatus.setDeliveryStatus(
                    FeatureSetProto.FeatureSetJobDeliveryStatus.STATUS_DELIVERED));

    boolean allDelivered =
        jobRepository.findByFeatureSetReference(ref).stream()
            .filter(Job::isRunning)
            .map(j -> j.getFeatureSetDeliveryStatuses().get(ref))
            .allMatch(
                js ->
                    js.getDeliveryStatus()
                        .equals(FeatureSetProto.FeatureSetJobDeliveryStatus.STATUS_DELIVERED));

    if (allDelivered) {
      log.info("FeatureSet {} update is completely delivered", ref);

      specService.updateFeatureSetStatus(
          CoreServiceProto.UpdateFeatureSetStatusRequest.newBuilder()
              .setReference(
                  FeatureSetReferenceProto.FeatureSetReference.newBuilder()
                      .setName(ref.getFeatureSetName())
                      .setProject(ref.getProjectName())
                      .build())
              .setStatus(FeatureSetProto.FeatureSetStatus.STATUS_READY)
              .build());
    }
  }
}
