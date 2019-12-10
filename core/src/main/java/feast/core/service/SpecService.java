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
import static feast.core.validators.Matchers.checkValidFeatureSetFilterName;

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
import feast.core.FeatureSetProto.FeatureSetSpec;
import feast.core.SourceProto;
import feast.core.StoreProto;
import feast.core.dao.FeatureSetRepository;
import feast.core.dao.StoreRepository;
import feast.core.exception.RetrievalException;
import feast.core.model.FeatureSet;
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
  private final StoreRepository storeRepository;
  private final Source defaultSource;

  private final Pattern versionPattern =
      Pattern.compile("^(?<comparator>[\\>\\<\\=]{0,2})(?<version>\\d*)$");

  @Autowired
  public SpecService(
      FeatureSetRepository featureSetRepository,
      StoreRepository storeRepository,
      Source defaultSource) {
    this.featureSetRepository = featureSetRepository;
    this.storeRepository = storeRepository;
    this.defaultSource = defaultSource;
  }

  /**
   * Get a feature set matching the feature name and version provided in the filter. The name is
   * required. If the version is provided then it will be used for the lookup. If the version is
   * omitted then the latest version will be returned.
   *
   * @param GetFeatureSetRequest containing the name and version of the feature set
   * @return GetFeatureSetResponse containing a single feature set
   */
  public GetFeatureSetResponse getFeatureSet(GetFeatureSetRequest request)
      throws InvalidProtocolBufferException {

    // Validate input arguments
    checkValidCharacters(request.getName(), "featureSetName");
    if (request.getName().isEmpty()) {
      throw io.grpc.Status.INVALID_ARGUMENT
          .withDescription("No feature set name provided")
          .asRuntimeException();
    }
    if (request.getVersion() < 0) {
      throw io.grpc.Status.INVALID_ARGUMENT
          .withDescription("Version number cannot be less than 0")
          .asRuntimeException();
    }

    FeatureSet featureSet;

    // Filter the list based on version
    if (request.getVersion() == 0) {
      featureSet =
          featureSetRepository.findFirstFeatureSetByNameOrderByVersionDesc(request.getName());

      if (featureSet == null) {
        throw io.grpc.Status.NOT_FOUND
            .withDescription(String.format("Feature set with name \"%s\" could not be found.",
                request.getName()))
            .asRuntimeException();
      }
    } else {
      featureSet =
          featureSetRepository.findFeatureSetByNameAndVersion(
              request.getName(), request.getVersion());

      if (featureSet == null) {
        throw io.grpc.Status.NOT_FOUND
            .withDescription(String.format("Feature set with name \"%s\" and version \"%s\" could "
                + "not be found.", request.getName(), request.getVersion()))
            .asRuntimeException();
      }
    }


    // Only a single item in list, return successfully
    return GetFeatureSetResponse.newBuilder().setFeatureSet(featureSet.toProto()).build();
  }

  /**
   * Get featureSets matching the feature name and version provided in the filter. If the feature
   * name is not provided, the method will return all featureSets currently registered to Feast.
   *
   * <p>The feature set name in the filter accepts any valid regex string. All matching featureSets
   * will be returned.
   *
   * <p>The version filter is optional; If not provided, this method will return all featureSet
   * versions of the featureSet name provided. Valid version filters should optionally contain a
   * comparator (<, <=, >, etc) and a version number, e.g. 10, <10, >=1
   *
   * @param filter filter containing the desired featureSet name and version filter
   * @return ListFeatureSetsResponse with list of featureSets found matching the filter
   */
  public ListFeatureSetsResponse listFeatureSets(ListFeatureSetsRequest.Filter filter)
      throws InvalidProtocolBufferException {
    String name = filter.getFeatureSetName();
    checkValidFeatureSetFilterName(name, "featureSetName");
    List<FeatureSet> featureSets;
    if (name.equals("")) {
      featureSets = featureSetRepository.findAllByOrderByNameAscVersionAsc();
    } else {
      featureSets = featureSetRepository.findByNameWithWildcardOrderByNameAscVersionAsc(name.replace('*', '%'));
      featureSets =
          featureSets.stream()
              .filter(getVersionFilter(filter.getFeatureSetVersion()))
              .collect(Collectors.toList());
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
   * Adds the featureSet to the repository, and prepares the sink for the feature creator to write
   * to. If there is a change in the featureSet's schema or source, the featureSet version will be
   * incremented.
   *
   * <p>This function is idempotent. If no changes are detected in the incoming featureSet's schema,
   * this method will update the incoming featureSet spec with the latest version stored in the
   * repository, and return that.
   *
   * @param newFeatureSetSpec featureSet to add.
   */
  public ApplyFeatureSetResponse applyFeatureSet(FeatureSetSpec newFeatureSetSpec)
      throws InvalidProtocolBufferException {
    FeatureSetValidator.validateSpec(newFeatureSetSpec);
    List<FeatureSet> existingFeatureSets =
        featureSetRepository.findByName(newFeatureSetSpec.getName());

    if (existingFeatureSets.size() == 0) {
      newFeatureSetSpec = newFeatureSetSpec.toBuilder().setVersion(1).build();
    } else {
      existingFeatureSets = Ordering.natural().reverse().sortedCopy(existingFeatureSets);
      FeatureSet latest = existingFeatureSets.get(0);
      FeatureSet featureSet = FeatureSet.fromProto(newFeatureSetSpec);

      // If the featureSet remains unchanged, we do nothing.
      if (featureSet.equalTo(latest)) {
        return ApplyFeatureSetResponse.newBuilder()
            .setFeatureSet(latest.toProto())
            .setStatus(Status.NO_CHANGE)
            .build();
      }
      newFeatureSetSpec = newFeatureSetSpec.toBuilder().setVersion(latest.getVersion() + 1).build();
    }
    FeatureSet featureSet = FeatureSet.fromProto(newFeatureSetSpec);
    if (newFeatureSetSpec.getSource() == SourceProto.Source.getDefaultInstance()) {
      featureSet.setSource(defaultSource);
    }
    featureSetRepository.saveAndFlush(featureSet);

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
   * @throws InvalidProtocolBufferException
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
