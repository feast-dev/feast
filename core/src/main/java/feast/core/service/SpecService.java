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

import static feast.core.validators.Matchers.checkValidCharacters;
import static feast.core.validators.Matchers.checkValidCharactersAllowAsterisk;

import com.google.protobuf.InvalidProtocolBufferException;
import feast.core.dao.FeatureSetRepository;
import feast.core.dao.JobRepository;
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
import feast.proto.core.SourceProto;
import feast.proto.core.StoreProto;
import feast.proto.core.StoreProto.Store.Subscription;
import java.util.ArrayList;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * Facilitates management of specs within the Feast registry. This includes getting existing specs
 * and registering new specs.
 */
@Slf4j
@Service
public class SpecService {

  private final FeatureSetRepository featureSetRepository;
  private final ProjectRepository projectRepository;
  private final StoreRepository storeRepository;
  private final JobRepository jobRepository;
  private final Source defaultSource;

  @Autowired
  public SpecService(
      FeatureSetRepository featureSetRepository,
      StoreRepository storeRepository,
      ProjectRepository projectRepository,
      JobRepository jobRepository,
      Source defaultSource) {
    this.featureSetRepository = featureSetRepository;
    this.storeRepository = storeRepository;
    this.projectRepository = projectRepository;
    this.jobRepository = jobRepository;
    this.defaultSource = defaultSource;
  }

  /**
   * Get a feature set matching the feature name and version and project. The feature set name and
   * project are required, but version can be omitted by providing 0 for its value. If the version
   * is omitted, the latest feature set will be provided.
   *
   * @param request: GetFeatureSetRequest Request containing filter parameters.
   * @return Returns a GetFeatureSetResponse containing a feature set.
   */
  @Transactional
  public GetFeatureSetResponse getFeatureSet(GetFeatureSetRequest request)
      throws InvalidProtocolBufferException {

    // Validate input arguments
    checkValidCharacters(request.getName(), "featureSetName");

    if (request.getName().isEmpty()) {
      throw new IllegalArgumentException("No feature set name provided");
    }
    if (request.getProject().isEmpty()) {
      throw new IllegalArgumentException("No project provided");
    }

    FeatureSet featureSet;

    featureSet =
        featureSetRepository.findFeatureSetByNameAndProject_Name(
            request.getName(), request.getProject());

    if (featureSet == null) {
      throw new RetrievalException(
          String.format("Feature set with name \"%s\" could not be found.", request.getName()));
    }

    checkAndUpdateStatus(featureSet);

    return GetFeatureSetResponse.newBuilder().setFeatureSet(featureSet.toProto()).build();
  }

  /**
   * Return a list of feature sets matching the feature set name and project provided in the filter.
   * All fields are requried. Use '*' for all arguments in order to return all feature sets in all
   * projects.
   *
   * <p>Project name can be explicitly provided, or an asterisk can be provided to match all
   * projects. It is not possible to provide a combination of asterisks/wildcards and text.
   *
   * <p>The feature set name in the filter accepts an asterisk as a wildcard. All matching feature
   * sets will be returned. Regex is not supported. Explicitly defining a feature set name is not
   * possible if a project name is not set explicitly
   *
   * @param filter filter containing the desired featureSet name
   * @return ListFeatureSetsResponse with list of featureSets found matching the filter
   */
  @Transactional
  public ListFeatureSetsResponse listFeatureSets(ListFeatureSetsRequest.Filter filter)
      throws InvalidProtocolBufferException {
    String name = filter.getFeatureSetName();
    String project = filter.getProject();

    if (project.isEmpty() || name.isEmpty()) {
      throw new IllegalArgumentException(
          "Invalid listFeatureSetRequest, missing arguments. Must provide project and feature set name.");
    }

    checkValidCharactersAllowAsterisk(name, "featureSetName");
    checkValidCharactersAllowAsterisk(project, "projectName");

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
        checkAndUpdateStatus(featureSet);
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
   * repository, and return that.
   *
   * @param newFeatureSet Feature set that will be created or updated.
   */
  public ApplyFeatureSetResponse applyFeatureSet(FeatureSetProto.FeatureSet newFeatureSet)
      throws InvalidProtocolBufferException {

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

    List<Subscription> subs = newStoreProto.getSubscriptionsList();
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
   * Checks the status of the given feature set. If there are jobs populating values from this
   * feature set, and if for each source and sink pair, there is at least 1 job running, it sets the
   * feature set's status to STATUS_READY and updates the db with the new status.
   *
   * @param featureSet {@link FeatureSet}
   */
  private void checkAndUpdateStatus(FeatureSet featureSet) throws InvalidProtocolBufferException {
    // check if the job is ready
    List<Job> jobsForFeatureSet =
        jobRepository.findByFeatureSetsIn(Collections.singletonList(featureSet));
    if (jobsForFeatureSet.size() != 0) {

      List<List<Job>> jobsGroupedBySourceAndSink =
          jobsForFeatureSet.stream()
              .collect(
                  Collectors.groupingBy(Job::getSource, Collectors.groupingBy(Job::getSinkName)))
              .values()
              .stream()
              .flatMap(map -> map.values().stream())
              .collect(Collectors.toList());

      Long featureSetVersion = featureSet.getVersion();
      for (List<Job> jobs : jobsGroupedBySourceAndSink) {
        long jobsRunning =
            jobs.stream()
                .filter(job -> job.getStatus() == JobStatus.RUNNING)
                .filter(
                    job -> job.getFeatureSetVersion(featureSet.getId()).equals(featureSetVersion))
                .count();
        if (jobsRunning == 0) {
          if (featureSet.getStatus() == FeatureSetStatus.STATUS_READY) {
            featureSet.setStatus(FeatureSetStatus.STATUS_PENDING);
            featureSetRepository.save(featureSet);
          }
          return;
        }
      }

      if (featureSet.getStatus() != FeatureSetStatus.STATUS_READY) {
        featureSet.setStatus(FeatureSetStatus.STATUS_READY);
        featureSetRepository.save(featureSet);
      }
    }
  }
}
