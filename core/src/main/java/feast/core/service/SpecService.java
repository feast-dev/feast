/*
 * Copyright 2018 The Feast Authors
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
 *
 */

package feast.core.service;

import com.google.common.collect.Lists;
import feast.core.CoreServiceProto.GetFeatureSetsRequest;
import feast.core.CoreServiceProto.GetStoresRequest;
import feast.core.FeatureSetProto.FeatureSetSpec;
import feast.core.dao.FeatureSetRepository;
import feast.core.dao.StoreRepository;
import feast.core.exception.RetrievalException;
import feast.core.model.FeatureSet;
import feast.core.model.Store;
import feast.core.validators.FeatureSetValidator;
import java.util.Collections;
import java.util.List;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * Facilitates management of specs within the Feast registry. This includes getting existing specs
 * and registering new specs.
 */
@Slf4j
@Service
public class SpecService {

  private final FeatureSetRepository featureSetRepository;
  private final StoreRepository storeRepository;

  private final Pattern versionPattern = Pattern
      .compile("^(?<comparator>[\\>\\<\\=]{0,2})(?<version>\\d*)$");

  @Autowired
  public SpecService(
      FeatureSetRepository featureSetRepository,
      StoreRepository storeRepository) {
    this.featureSetRepository = featureSetRepository;
    this.storeRepository = storeRepository;

  }

  /**
   * Get featureSets matching the feature name and version provided in the filter. If the feature
   * name is not provided, the method will return all featureSets currently registered to Feast.
   *
   * The version filter is optional; If not provided, this method will return all featureSet
   * versions of the featureSet name provided. Valid version filters should optionally contain a
   * comparator (<, <=, >, etc) and a version number, e.g. 10, <10, >=1
   *
   * @param filter filter containing the desired featureSet name and version filter
   * @return List of featureSets found matching the filter
   */
  public List<FeatureSet> getFeatureSets(GetFeatureSetsRequest.Filter filter) {
    String name = filter.getFeatureSetName();
    if (name.equals("")) {
      return featureSetRepository.findAll();
    }
    List<FeatureSet> featureSets = featureSetRepository.findByName(name);
    featureSets = featureSets.stream().filter(getVersionFilter(filter.getFeatureSetVersion()))
        .collect(Collectors.toList());
    if (featureSets.size() == 0) {
      throw new RetrievalException(
          String.format("Unable to find any featureSets matching the filter '%s'", filter));
    }
    return featureSets;
  }

  /**
   * Get stores matching the store name provided in the filter. If the store name is not provided,
   * the method will return all stores currently registered to Feast.
   *
   * @param filter filter containing the desired store name
   * @return List of stores found matching the filter
   */
  public List<Store> getStores(GetStoresRequest.Filter filter) {
    String name = filter.getName();
    if (name.equals("")) {
      return storeRepository.findAll();
    }
    Store store = storeRepository.findById(name)
        .orElseThrow(() -> new RetrievalException(String.format("Store with name '%s' not found",
            name)));
    return Lists.newArrayList(store);
  }

  /**
   * Adds the featureSet to the repository. This function is idempotent. If no changes are detected in the incoming featureset, this method will do nothing.
   *
   * @param newFeatureSetSpec featureSet to add.
   */
  public void applyFeatureSet(FeatureSetSpec newFeatureSetSpec) {
    FeatureSetValidator.validateSpec(newFeatureSetSpec);
    List<FeatureSet> existingFeatureSets = featureSetRepository
        .findByName(newFeatureSetSpec.getName());
    if (existingFeatureSets.size() == 0) {
      newFeatureSetSpec = newFeatureSetSpec.toBuilder().setVersion(1).build();
    } else {
      Collections.sort(existingFeatureSets, Collections.reverseOrder());
      FeatureSet latest = existingFeatureSets.get(0);

      // If the featureset remains unchanged, we do nothing.
      if (featureSetUnchanged(latest.toProto(), newFeatureSetSpec)) {
        return;
      }
      newFeatureSetSpec = newFeatureSetSpec.toBuilder()
          .setVersion(latest.getVersion() + 1)
          .build();
    }
    FeatureSet featureSet = FeatureSet.fromProto(newFeatureSetSpec);
    featureSetRepository.save(featureSet);
  }

  private boolean featureSetUnchanged(FeatureSetSpec latest, FeatureSetSpec inc) {
    FeatureSetSpec featureSetSpecWithVersion = inc.toBuilder()
        .setVersion(latest.getVersion()).build();
    return latest.equals(featureSetSpecWithVersion);
  }

  private Predicate<? super FeatureSet> getVersionFilter(String versionFilter) {
    if (versionFilter.equals("")) {
      return v -> true;
    }
    Matcher match = versionPattern.matcher(versionFilter);
    match.find();

    if (!match.matches()) {
      throw new RetrievalException(
          String.format(
              "Invalid version string '%s' provided. Version string may either "
                  + "be a fixed version, e.g. 10, or contain a comparator, e.g. >10.",
              versionFilter));
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
        throw new RetrievalException(
            String.format(
                "Invalid comparator '%s' provided. Version string may either "
                    + "be a fixed version, e.g. 10, or contain a comparator, e.g. >10.",
                comparator));
    }
  }
}

//
//  private void startOrUpdateJobs(EntityInfo entityInfo) {
//    List<FeatureInfo> features = featureInfoRepository.findByEntityName(entityInfo.getName());
//    List<FeatureSpec> featureSpecs = features.stream().map(FeatureInfo::getFeatureSpec)
//        .collect(Collectors.toList());
//    for (StorageSpec storageSpec : storageSpecs.getSinks()) {
//      ImportJobSpecs importJobSpecs = jobCoordinatorService
//          .createImportJobSpecs(entityInfo.getTopic().getName(), entityInfo.getEntitySpec(),
//              featureSpecs, storageSpec, storageSpecs.getErrorsStorageSpec());
//      boolean exists = runningJobExistsForSinkId(entityInfo.getJobs(), storageSpec.getId());
//      if (exists) {
//        // if job exists, update
//        JobInfo existingJob = entityInfo.getJobs().get(0);
//        importJobSpecs = importJobSpecs.toBuilder().setJobId(existingJob.getId()).build();
//        jobCoordinatorService.updateJob(existingJob, importJobSpecs);
//      } else {
//        // if job doesn't exist, create
//        JobInfo job = jobCoordinatorService.startJob(importJobSpecs);
//        List<JobInfo> existingJobs = entityInfo.getJobs();
//        existingJobs.add(job);
//        entityInfo.setJobs(existingJobs);
//        entityInfoRepository.saveAndFlush(entityInfo);
//      }
//    }
//  }
//
//  private boolean runningJobExistsForSinkId(List<JobInfo> jobs, String sinkId) {
//    return jobs.stream().filter(job -> job.getStatus().equals(JobStatus.RUNNING))
//        .anyMatch(job -> job.getSinkId().equals(sinkId));
//  }
//
//  private FeatureInfo updateOrCreateFeature(FeatureInfo featureInfo, FeatureSpec spec)
//      throws InvalidProtocolBufferException {
//    Action action;
//    if (featureInfo != null) {
//      featureInfo.update(spec);
//      action = Action.UPDATE;
//    } else {
//      EntityInfo entity = entityInfoRepository.findById(spec.getEntity()).orElse(null);
//      FeatureGroupInfo featureGroupInfo =
//          featureGroupInfoRepository.findById(spec.getGroup()).orElse(null);
//      featureInfo = new FeatureInfo(spec, entity, featureGroupInfo);
//      action = Action.REGISTER;
//    }
//    FeatureInfo out = featureInfoRepository.save(featureInfo);
//    if (!out.getId().equals(spec.getId())) {
//      throw new RegistrationException("failed to register or update feature");
//    }
//    AuditLogger.log(
//        Resource.FEATURE,
//        spec.getId(),
//        action,
//        "Feature applied: %s",
//        JsonFormat.printer().print(spec));
//    return out;
//  }

