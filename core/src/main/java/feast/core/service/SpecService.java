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
import static feast.core.validators.Matchers.checkValidCharacters;
import static feast.core.validators.Matchers.checkValidCharactersAllowAsterisk;

import com.google.protobuf.InvalidProtocolBufferException;
import feast.core.dao.FeatureSetRepository;
import feast.core.dao.ProjectRepository;
import feast.core.dao.StoreRepository;
import feast.core.exception.RetrievalException;
import feast.core.model.*;
import feast.core.validators.FeatureSetValidator;
import feast.proto.core.CoreServiceProto.ApplyFeatureSetResponse;
import feast.proto.core.CoreServiceProto.ApplyFeatureSetResponse.Status;
import feast.proto.core.CoreServiceProto.GetFeatureSetRequest;
import feast.proto.core.CoreServiceProto.GetFeatureSetResponse;
import feast.proto.core.CoreServiceProto.ListFeatureSetsRequest;
import feast.proto.core.CoreServiceProto.ListFeatureSetsResponse;
import feast.proto.core.CoreServiceProto.ListStoresRequest;
import feast.proto.core.CoreServiceProto.ListStoresResponse;
import feast.proto.core.CoreServiceProto.ListStoresResponse.Builder;
import feast.proto.core.CoreServiceProto.UpdateStoreRequest;
import feast.proto.core.CoreServiceProto.UpdateStoreResponse;
import feast.proto.core.FeatureSetProto;
import feast.proto.core.FeatureSetProto.FeatureSetStatus;
import feast.proto.core.IngestionJobProto;
import feast.proto.core.SourceProto;
import feast.proto.core.StoreProto;
import feast.proto.core.StoreProto.Store.Subscription;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.util.Pair;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * Facilitates management of specs within the Feast registry. This includes getting existing specs
 * and registering new specs.
 */
@Slf4j
@Service
public class SpecService {

  private final int SPEC_PUBLISHING_TIMEOUT_SECONDS = 5;

  private final FeatureSetRepository featureSetRepository;
  private final ProjectRepository projectRepository;
  private final StoreRepository storeRepository;
  private final Source defaultSource;
  private final KafkaTemplate<String, FeatureSetProto.FeatureSetSpec> specPublisher;

  @Autowired
  public SpecService(
      FeatureSetRepository featureSetRepository,
      StoreRepository storeRepository,
      ProjectRepository projectRepository,
      Source defaultSource,
      KafkaTemplate<String, FeatureSetProto.FeatureSetSpec> specPublisher) {
    this.featureSetRepository = featureSetRepository;
    this.storeRepository = storeRepository;
    this.projectRepository = projectRepository;
    this.defaultSource = defaultSource;
    this.specPublisher = specPublisher;
  }

  /**
   * Get a feature set matching the feature name and version and project. The feature set name and
   * project are required, but version can be omitted by providing 0 for its value. If the version
   * is omitted, the latest feature set will be provided. If the project is omitted, the default
   * would be used.
   *
   * @param request: GetFeatureSetRequest Request containing filter parameters.
   * @return Returns a GetFeatureSetResponse containing a feature set..
   */
  public GetFeatureSetResponse getFeatureSet(GetFeatureSetRequest request)
      throws InvalidProtocolBufferException {

    // Validate input arguments
    checkValidCharacters(request.getName(), "featureSetName");

    if (request.getName().isEmpty()) {
      throw new IllegalArgumentException("No feature set name provided");
    }
    // Autofill default project if project is not specified
    if (request.getProject().isEmpty()) {
      request = request.toBuilder().setProject(Project.DEFAULT_NAME).build();
    }

    FeatureSet featureSet;

    featureSet =
        featureSetRepository.findFeatureSetByNameAndProject_Name(
            request.getName(), request.getProject());

    if (featureSet == null) {
      throw new RetrievalException(
          String.format("Feature set with name \"%s\" could not be found.", request.getName()));
    }
    return GetFeatureSetResponse.newBuilder().setFeatureSet(featureSet.toProto()).build();
  }

  /**
   * Return a list of feature sets matching the feature set name and project provided in the filter.
   * All fields are requried. Use '*' for all arguments in order to return all feature sets in all
   * projects.
   *
   * <p>Project name can be explicitly provided, or an asterisk can be provided to match all
   * projects. It is not possible to provide a combination of asterisks/wildcards and text. If the
   * project name is omitted, the default project would be used.
   *
   * <p>The feature set name in the filter accepts an asterisk as a wildcard. All matching feature
   * sets will be returned. Regex is not supported. Explicitly defining a feature set name is not
   * possible if a project name is not set explicitly
   *
   * @param filter filter containing the desired featureSet name
   * @return ListFeatureSetsResponse with list of featureSets found matching the filter
   */
  public ListFeatureSetsResponse listFeatureSets(ListFeatureSetsRequest.Filter filter)
      throws InvalidProtocolBufferException {
    String name = filter.getFeatureSetName();
    String project = filter.getProject();

    if (name.isEmpty()) {
      throw new IllegalArgumentException(
          "Invalid listFeatureSetRequest, missing arguments. Must provide feature set name:");
    }

    checkValidCharactersAllowAsterisk(name, "featureSetName");
    checkValidCharactersAllowAsterisk(project, "projectName");

    // Autofill default project if project not specified
    if (project.isEmpty()) {
      project = Project.DEFAULT_NAME;
    }

    List<FeatureSet> featureSets = new ArrayList<FeatureSet>() {};

    if (project.contains("*")) {
      // Matching a wildcard project
      if (name.contains("*")) {
        featureSets =
            featureSetRepository.findAllByNameLikeAndProject_NameLikeOrderByNameAsc(
                name.replace('*', '%'), project.replace('*', '%'));
      } else {
        throw new IllegalArgumentException(
            String.format(
                "Invalid listFeatureSetRequest. Feature set name must be set to "
                    + "\"*\" if the project name and feature set name aren't set explicitly: \n%s",
                filter.toString()));
      }
    } else if (!project.contains("*")) {
      // Matching a specific project
      if (name.contains("*")) {
        // Find all feature sets matching a pattern in a specific project
        featureSets =
            featureSetRepository.findAllByNameLikeAndProject_NameOrderByNameAsc(
                name.replace('*', '%'), project);

      } else if (!name.contains("*")) {
        // Find a specific feature set in a specific project
        FeatureSet featureSet =
            featureSetRepository.findFeatureSetByNameAndProject_Name(name, project);
        if (featureSet != null) {
          featureSets.add(featureSet);
        }
      }
    } else {
      throw new IllegalArgumentException(
          String.format(
              "Invalid listFeatureSetRequest. Project name cannot be a pattern. It may only be"
                  + "a specific project name or an asterisk: \n%s",
              filter.toString()));
    }

    ListFeatureSetsResponse.Builder response = ListFeatureSetsResponse.newBuilder();
    if (featureSets.size() > 0) {
      for (FeatureSet featureSet : featureSets) {
        response.addFeatureSets(featureSet.toProto());
      }
    }

    return response.build();
  }

  /**
   * Get stores matching the store name provided in the filter. If the store name is not provided,
   * the method will return all stores currently registered to Feast.
   *
   * @param filter filter containing the desired store name
   * @return ListStoresResponse containing list of stores found matching the filter
   */
  @Transactional
  public ListStoresResponse listStores(ListStoresRequest.Filter filter) {
    try {
      String name = filter.getName();
      if (name.equals("")) {
        Builder responseBuilder = ListStoresResponse.newBuilder();
        for (Store store : storeRepository.findAll()) {
          responseBuilder.addStore(store.toProto());
        }
        return responseBuilder.build();
      }
      Store store =
          storeRepository
              .findById(name)
              .orElseThrow(
                  () ->
                      new RetrievalException(
                          String.format("Store with name '%s' not found", name)));
      return ListStoresResponse.newBuilder().addStore(store.toProto()).build();
    } catch (InvalidProtocolBufferException e) {
      throw io.grpc.Status.NOT_FOUND
          .withDescription("Unable to retrieve stores")
          .withCause(e)
          .asRuntimeException();
    }
  }

  /**
   * Creates or updates a feature set in the repository.
   *
   * <p>This function is idempotent. If no changes are detected in the incoming featureSet's schema,
   * this method will update the incoming featureSet spec with the latest version stored in the
   * repository, and return that. If project is not specified in the given featureSet, will assign
   * the featureSet to the'default' project.
   *
   * @param newFeatureSet Feature set that will be created or updated.
   */
  @Transactional
  public ApplyFeatureSetResponse applyFeatureSet(FeatureSetProto.FeatureSet newFeatureSet)
      throws InvalidProtocolBufferException {
    // Autofill default project if not specified
    if (newFeatureSet.getSpec().getProject().isEmpty()) {
      newFeatureSet =
          newFeatureSet
              .toBuilder()
              .setSpec(newFeatureSet.getSpec().toBuilder().setProject(Project.DEFAULT_NAME).build())
              .build();
    }

    // Validate incoming feature set
    FeatureSetValidator.validateSpec(newFeatureSet);

    // Find project or create new one if it does not exist
    String project_name = newFeatureSet.getSpec().getProject();
    Project project =
        projectRepository
            .findById(newFeatureSet.getSpec().getProject())
            .orElse(new Project(project_name));

    // Ensure that the project retrieved from repository is not archived
    if (project.isArchived()) {
      throw new IllegalArgumentException(String.format("Project is archived: %s", project_name));
    }

    // Set source to default if not set in proto
    if (newFeatureSet.getSpec().getSource() == SourceProto.Source.getDefaultInstance()) {
      newFeatureSet =
          newFeatureSet
              .toBuilder()
              .setSpec(
                  newFeatureSet.getSpec().toBuilder().setSource(defaultSource.toProto()).build())
              .build();
    }

    // Retrieve existing FeatureSet
    FeatureSet featureSet =
        featureSetRepository.findFeatureSetByNameAndProject_Name(
            newFeatureSet.getSpec().getName(), project_name);

    Status status;
    if (featureSet == null) {
      // Create new feature set since it doesn't exist
      newFeatureSet = newFeatureSet.toBuilder().setSpec(newFeatureSet.getSpec()).build();
      featureSet = FeatureSet.fromProto(newFeatureSet);
      status = Status.CREATED;
    } else {
      // If the featureSet remains unchanged, we do nothing.
      if (featureSet.toProto().getSpec().equals(newFeatureSet.getSpec())) {
        return ApplyFeatureSetResponse.newBuilder()
            .setFeatureSet(featureSet.toProto())
            .setStatus(Status.NO_CHANGE)
            .build();
      }
      featureSet.updateFromProto(newFeatureSet);
      status = Status.UPDATED;
    }

    featureSet.incVersion();

    // Sending latest version of FeatureSet to all currently running IngestionJobs (there's one
    // topic for all sets).
    // All related jobs would apply new FeatureSet on the fly.
    // We wait for Kafka broker to ack that the message was added to topic before actually
    // committing this FeatureSet.
    // In case kafka doesn't respond within SPEC_PUBLISHING_TIMEOUT_SECONDS we abort current
    // transaction and return error to client.
    try {
      specPublisher
          .sendDefault(featureSet.getReference(), featureSet.toProto().getSpec())
          .get(SPEC_PUBLISHING_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    } catch (Exception e) {
      throw io.grpc.Status.UNAVAILABLE
          .withDescription(
              String.format(
                  "Unable to publish FeatureSet to Kafka. Cause: %s",
                  e.getCause() != null ? e.getCause().getMessage() : "unknown"))
          .withCause(e)
          .asRuntimeException();
    }

    // Updating delivery status for related jobs (that are currently using this FeatureSet).
    // We now set status to IN_PROGRESS, so listenAckFromJobs would be able to
    // monitor delivery progress for each new version.
    featureSet.getJobStatuses().stream()
        .filter(s -> s.getJob().isRunning())
        .forEach(
            s ->
                s.setDeliveryStatus(
                    FeatureSetProto.FeatureSetJobDeliveryStatus.STATUS_IN_PROGRESS));

    // Persist the FeatureSet object
    featureSet.setStatus(FeatureSetStatus.STATUS_PENDING);
    project.addFeatureSet(featureSet);
    projectRepository.saveAndFlush(project);

    // Build ApplyFeatureSetResponse
    return ApplyFeatureSetResponse.newBuilder()
        .setFeatureSet(featureSet.toProto())
        .setStatus(status)
        .build();
  }

  /**
   * UpdateStore updates the repository with the new given store.
   *
   * @param updateStoreRequest containing the new store definition
   * @return UpdateStoreResponse containing the new store definition
   */
  @Transactional
  public UpdateStoreResponse updateStore(UpdateStoreRequest updateStoreRequest)
      throws InvalidProtocolBufferException {
    StoreProto.Store newStoreProto = updateStoreRequest.getStore();
    Store store = Store.fromProto(newStoreProto);

    List<Subscription> subs = store.getSubscriptionsByStr(false);
    for (Subscription sub : subs) {
      // Ensure that all fields in a subscription contain values
      if ((sub.getName().isEmpty()) || sub.getProject().isEmpty()) {
        throw new IllegalArgumentException(
            String.format("Missing parameter in subscription: %s", sub));
      }
    }
    Store existingStore = storeRepository.findById(newStoreProto.getName()).orElse(null);

    // Do nothing if no change
    if (existingStore != null && existingStore.toProto().equals(newStoreProto)) {
      return UpdateStoreResponse.newBuilder()
          .setStatus(UpdateStoreResponse.Status.NO_CHANGE)
          .setStore(updateStoreRequest.getStore())
          .build();
    }

    Store newStore = Store.fromProto(newStoreProto);
    storeRepository.save(newStore);
    return UpdateStoreResponse.newBuilder()
        .setStatus(UpdateStoreResponse.Status.UPDATED)
        .setStore(updateStoreRequest.getStore())
        .build();
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

    if (featureSet.getVersion() != record.value().getFeatureSetVersion()) {
      log.warn(
          String.format(
              "ACKListener received outdated ack for %s. Current %d, Received %d",
              setReference, featureSet.getVersion(), record.value().getFeatureSetVersion()));
      return;
    }

    featureSet.getJobStatuses().stream()
        .filter(js -> js.getJob().getId().equals(record.value().getJobName()))
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
      featureSet.setStatus(FeatureSetStatus.STATUS_READY);
      featureSetRepository.saveAndFlush(featureSet);
    }
  }
}
