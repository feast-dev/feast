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

import com.google.protobuf.InvalidProtocolBufferException;
import feast.core.config.FeastProperties;
import feast.core.config.FeastProperties.JobProperties;
import feast.core.dao.FeatureSetRepository;
import feast.core.dao.JobRepository;
import feast.core.job.JobManager;
import feast.core.job.JobUpdateTask;
import feast.core.model.*;
import feast.proto.core.CoreServiceProto.ListStoresRequest.Filter;
import feast.proto.core.CoreServiceProto.ListStoresResponse;
import feast.proto.core.FeatureSetProto;
import feast.proto.core.IngestionJobProto;
import feast.proto.core.StoreProto;
import feast.proto.core.StoreProto.Store.Subscription;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.*;
import java.util.stream.Collectors;
import javax.validation.constraints.Positive;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.util.Pair;
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
  private final SpecService specService;
  private final JobManager jobManager;
  private final JobProperties jobProperties;
  private final KafkaTemplate<String, FeatureSetProto.FeatureSetSpec> specPublisher;

  @Autowired
  public JobCoordinatorService(
      JobRepository jobRepository,
      FeatureSetRepository featureSetRepository,
      SpecService specService,
      JobManager jobManager,
      FeastProperties feastProperties,
      KafkaTemplate<String, FeatureSetProto.FeatureSetSpec> specPublisher) {
    this.jobRepository = jobRepository;
    this.featureSetRepository = featureSetRepository;
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
    @Positive long updateTimeout = jobProperties.getJobUpdateTimeoutSeconds();
    List<JobUpdateTask> jobUpdateTasks = new ArrayList<>();
    ListStoresResponse listStoresResponse = specService.listStores(Filter.newBuilder().build());

    for (StoreProto.Store storeSpec : listStoresResponse.getStoreList()) {
      Set<FeatureSet> featureSets = new HashSet<>();
      Store store = Store.fromProto(storeSpec);

      for (Subscription subscription : store.getSubscriptions()) {
        List<FeatureSet> featureSetsForSub =
            featureSetRepository.findAllByNameLikeAndProject_NameLikeOrderByNameAsc(
                subscription.getName().replace('*', '%'),
                subscription.getProject().replace('*', '%'));
        featureSets.addAll(featureSetsForSub);
      }

      featureSets.stream()
          .collect(Collectors.groupingBy(FeatureSet::getSource))
          .forEach(
              (source, setsForSource) -> {
                Optional<Job> originalJob = getJob(source, store);
                jobUpdateTasks.add(
                    new JobUpdateTask(
                        setsForSource, source, store, originalJob, jobManager, updateTimeout));
              });
    }
    if (jobUpdateTasks.isEmpty()) {
      log.info("No jobs found.");
      return;
    }

    log.info("Creating/Updating {} jobs...", jobUpdateTasks.size());
    startOrUpdateJobs(jobUpdateTasks);
  }

  void startOrUpdateJobs(List<JobUpdateTask> tasks) {
    ExecutorService executorService = Executors.newFixedThreadPool(tasks.size());
    ExecutorCompletionService<Job> ecs = new ExecutorCompletionService<>(executorService);
    tasks.forEach(ecs::submit);

    int completedTasks = 0;
    List<Job> startedJobs = new ArrayList<>();
    while (completedTasks < tasks.size()) {
      try {
        Job job = ecs.take().get();
        if (job != null) {
          startedJobs.add(job);
        }
      } catch (ExecutionException | InterruptedException e) {
        log.warn("Unable to start or update job: {}", e.getMessage());
      }
      completedTasks++;
    }
    jobRepository.saveAll(startedJobs);
    executorService.shutdown();
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
            projectAndSetName.getSecond(), projectAndSetName.getFirst());
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
