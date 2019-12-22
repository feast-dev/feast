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

import com.google.common.collect.Ordering;
import com.google.protobuf.InvalidProtocolBufferException;
import feast.core.CoreServiceProto.ApplyFeatureSetResponse;
import feast.core.CoreServiceProto.ApplyFeatureSetResponse.Status;
import feast.core.CoreServiceProto.GetFeatureSetRequest;
import feast.core.CoreServiceProto.GetFeatureSetResponse;
import feast.core.CoreServiceProto.ListFeatureSetsRequest;
import feast.core.CoreServiceProto.ListFeatureSetsResponse;
import feast.core.CoreServiceProto.ListStoresRequest;
import feast.core.CoreServiceProto.ListStoresResponse;
import feast.core.CoreServiceProto.ListStoresResponse.Builder;
import feast.core.CoreServiceProto.UpdateStoreRequest;
import feast.core.CoreServiceProto.UpdateStoreResponse;
import feast.core.FeatureSetProto;
import feast.core.SourceProto;
import feast.core.StoreProto;
import feast.core.dao.FeatureSetRepository;
import feast.core.dao.ProjectRepository;
import feast.core.dao.StoreRepository;
import feast.core.exception.RetrievalException;
import feast.core.model.FeatureSet;
import feast.core.model.Project;
import feast.core.model.Source;
import feast.core.model.Store;
import feast.core.validators.FeatureSetValidator;
import java.util.List;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
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
  private final Source defaultSource;

  private final Pattern versionPattern =
      Pattern.compile("^(?<comparator>[\\>\\<\\=]{0,2})(?<version>\\d*)$");

  @Autowired
  public SpecService(
      FeatureSetRepository featureSetRepository,
      StoreRepository storeRepository,
      ProjectRepository projectRepository,
      Source defaultSource) {
    this.featureSetRepository = featureSetRepository;
    this.storeRepository = storeRepository;
    this.projectRepository = projectRepository;
    this.defaultSource = defaultSource;
  }

  /**
   * Get a feature set matching the feature name and version and project. The feature set name and
   * project are required, but version can be omitted by providing 0 for its value. If the version
   * is omitted, the latest feature set will be provided.
   *
   * @param request: GetFeatureSetRequest Request containing filter parameters.
   * @return Returns a GetFeatureSetResponse containing a feature set..
   */
  public GetFeatureSetResponse getFeatureSet(GetFeatureSetRequest request) {

    // Validate input arguments
    checkValidCharacters(request.getName(), "featureSetName");

    if (request.getName().isEmpty()) {
      throw new IllegalArgumentException("No feature set name provided");
    }
    if (request.getProject().isEmpty()) {
      throw new IllegalArgumentException("No project provided");
    }
    if (request.getVersion() < 0) {
      throw new IllegalArgumentException("Version number cannot be less than 0");
    }

    FeatureSet featureSet;

    // Filter the list based on version
    if (request.getVersion() == 0) {
      featureSet =
          featureSetRepository
              .findFirstFeatureSetByNameAndProject_NameOrderByVersionDesc(request.getName(),
                  request.getProject());

      if (featureSet == null) {
        throw new NullPointerException(String.format(
            "Feature set with name \"%s\" could not be found.", request.getName()));
      }
    } else {
      featureSet =
          featureSetRepository.findFeatureSetByNameAndProject_NameAndVersion(
              request.getName(), request.getProject(), request.getVersion());

      if (featureSet == null) {
        throw new NullPointerException(String.format(
            "Feature set with name \"%s\" and version \"%s\" could " + "not be found.",
            request.getName(), request.getVersion()));
      }
    }

    // Only a single item in list, return successfully
    return GetFeatureSetResponse.newBuilder().setFeatureSet(featureSet.toProto()).build();
  }


  /**
   * Return a list of feature sets matching the feature set name, version, and project provided in
   * the filter. Providing a project name will filter feature sets to only a single project.
   * Providing a feature set name will match only feature sets with that name, but can return
   * multiple versions. If a feature set name is not provided then all names will be returned.
   *
   * <p>The feature set name in the filter accepts an asterisk as a wildcard. All matching
   * featureSets will be returned.
   *
   * <p>The version filter is optional; If not provided, this method will return all featureSet
   * versions of the featureSet name provided. Valid version filters should optionally contain a
   * comparator (<, <=, >, etc) and a version number, e.g. 10, <10, >=1
   *
   * @param filter filter containing the desired featureSet name and version filter
   * @return ListFeatureSetsResponse with list of featureSets found matching the filter
   */
  public ListFeatureSetsResponse listFeatureSets(ListFeatureSetsRequest.Filter filter) {
    String name = filter.getFeatureSetName();
    String project = filter.getProject();
    String version = filter.getFeatureSetVersion();
    checkValidCharactersAllowAsterisk(name, "featureSetName");
    checkValidCharacters(project, "projectName");

    name = name.replace('*', '%');
    List<FeatureSet> featureSets;

    if (project.isEmpty() && name.isEmpty() && version.isEmpty()) {
      // Find all, filter archived
      featureSets = featureSetRepository.findAllByProject_ArchivedOrderByNameAscVersionAsc(false);

    } else if (!project.isEmpty() && name.isEmpty() && version.isEmpty()) {
      // Find by project, filter archived
      featureSets = featureSetRepository
          .findAllByProject_NameAndProject_ArchivedOrderByNameAscVersionAsc(project, false);

    } else if (!project.isEmpty() && !name.isEmpty() && version.isEmpty()) {
      // Find by project and feature set name, filter archived
      featureSets = featureSetRepository
          .findAllByNameLikeAndProject_NameAndProject_ArchivedOrderByNameAscVersionAsc(name,
              project, false);

    } else if (!project.isEmpty() && !name.isEmpty() && !version.isEmpty()) {
      // Find by project, feature set name, and specific version, filter archived
      featureSets =
          featureSetRepository.findByNameWithWildcardOrderByNameAscVersionAsc(
              project, name);
      featureSets =
          featureSets.stream()
              .filter(getVersionFilter(filter.getFeatureSetVersion()))
              .collect(Collectors.toList());

    } else {
      throw new IllegalArgumentException(
          String.format("Invalid listFeatureSetRequest %s", filter.toString()));
    }

    ListFeatureSetsResponse.Builder response = ListFeatureSetsResponse.newBuilder();
    for (FeatureSet featureSet : featureSets) {
      response.addFeatureSets(featureSet.toProto());
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
   * Creates or updates a feature set in the repository. If there is a change in the feature set
   * schema, then the feature set version will be incremented.
   *
   * <p>This function is idempotent. If no changes are detected in the incoming featureSet's
   * schema, this method will update the incoming featureSet spec with the latest version stored in
   * the repository, and return that.
   *
   * @param newFeatureSet Feature set that will be created or updated.
   */
  public ApplyFeatureSetResponse applyFeatureSet(FeatureSetProto.FeatureSet newFeatureSet) {

    // Validate incoming feature set
    FeatureSetValidator.validateSpec(newFeatureSet);

    // Ensure that the project already exists
    String project_name = newFeatureSet.getSpec().getProject();
    Project project = projectRepository.findById(newFeatureSet.getSpec().getProject())
        .orElseThrow(() -> new IllegalArgumentException(String
            .format("Project name does not exist. Please create a project first: %s", project_name
            )));

    // Ensure that the project is not archived
    if (project.isArchived()) {
      throw new IllegalArgumentException(String
          .format("Project is archived: %s", project_name
          ));
    }

    // Retrieve all existing FeatureSet objects
    List<FeatureSet> existingFeatureSets =
        featureSetRepository
            .findAllByNameLikeAndProject_NameAndProject_ArchivedOrderByNameAscVersionAsc(
                newFeatureSet.getSpec().getName(), project_name, false);

    if (existingFeatureSets.size() == 0) {
      // Create new feature set since it doesn't exist
      newFeatureSet = newFeatureSet.toBuilder()
          .setSpec(newFeatureSet.getSpec().toBuilder().setVersion(1)).build();
    } else {
      // Retrieve the latest feature set if the name does exist
      existingFeatureSets = Ordering.natural().reverse().sortedCopy(existingFeatureSets);
      FeatureSet latest = existingFeatureSets.get(0);
      FeatureSet featureSet = FeatureSet.fromProto(newFeatureSet);

      // If the featureSet remains unchanged, we do nothing.
      if (featureSet.equalTo(latest)) {
        return ApplyFeatureSetResponse.newBuilder()
            .setFeatureSet(latest.toProto())
            .setStatus(Status.NO_CHANGE)
            .build();
      }
      // TODO: There is a race condition here with incrementing the version
      newFeatureSet = newFeatureSet.toBuilder()
          .setSpec(newFeatureSet.getSpec().toBuilder().setVersion(latest.getVersion() + 1)).build();
    }

    // Build a new FeatureSet object which includes the new properties
    FeatureSet featureSet = FeatureSet.fromProto(newFeatureSet);
    if (newFeatureSet.getSpec().getSource() == SourceProto.Source.getDefaultInstance()) {
      featureSet.setSource(defaultSource);
    }

    // Persist the FeatureSet object
    project.addFeatureSet(featureSet);
    projectRepository.saveAndFlush(project);

    // Build ApplyFeatureSetResponse
    return ApplyFeatureSetResponse.newBuilder()
        .setFeatureSet(featureSet.toProto())
        .setStatus(Status.CREATED)
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

  private Predicate<? super FeatureSet> getVersionFilter(String versionFilter) {
    if (versionFilter.equals("")) {
      return v -> true;
    }
    Matcher match = versionPattern.matcher(versionFilter);
    match.find();

    if (!match.matches()) {
      throw io.grpc.Status.INVALID_ARGUMENT
          .withDescription(
              String.format(
                  "Invalid version string '%s' provided. Version string may either "
                      + "be a fixed version, e.g. 10, or contain a comparator, e.g. >10.",
                  versionFilter))
          .asRuntimeException();
    }

    int versionNumber = Integer.valueOf(match.group("version"));
    String comparator = match.group("comparator");
    switch (comparator) {
      case "<":
        return v -> v.getVersion() < versionNumber;
      case ">":
        return v -> v.getVersion() > versionNumber;
      case "<=":
        return v -> v.getVersion() <= versionNumber;
      case ">=":
        return v -> v.getVersion() >= versionNumber;
      case "":
        return v -> v.getVersion() == versionNumber;
      default:
        throw io.grpc.Status.INVALID_ARGUMENT
            .withDescription(
                String.format(
                    "Invalid comparator '%s' provided. Version string may either "
                        + "be a fixed version, e.g. 10, or contain a comparator, e.g. >10.",
                    comparator))
            .asRuntimeException();
    }
  }
}
