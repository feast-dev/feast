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

package feast.core.http;

import feast.core.JobServiceProto.JobServiceTypes.GetJobResponse;
import feast.core.JobServiceProto.JobServiceTypes.ListJobsResponse;
import feast.core.UIServiceProto.UIServiceTypes.EntityDetail;
import feast.core.UIServiceProto.UIServiceTypes.FeatureDetail;
import feast.core.UIServiceProto.UIServiceTypes.FeatureGroupDetail;
import feast.core.UIServiceProto.UIServiceTypes.GetEntityResponse;
import feast.core.UIServiceProto.UIServiceTypes.GetFeatureGroupResponse;
import feast.core.UIServiceProto.UIServiceTypes.GetFeatureResponse;
import feast.core.UIServiceProto.UIServiceTypes.GetStorageResponse;
import feast.core.UIServiceProto.UIServiceTypes.ListEntitiesResponse;
import feast.core.UIServiceProto.UIServiceTypes.ListFeatureGroupsResponse;
import feast.core.UIServiceProto.UIServiceTypes.ListFeaturesResponse;
import feast.core.UIServiceProto.UIServiceTypes.ListStorageResponse;
import feast.core.UIServiceProto.UIServiceTypes.StorageDetail;
import feast.core.model.EntityInfo;
import feast.core.model.FeatureGroupInfo;
import feast.core.model.FeatureInfo;
import feast.core.model.StorageInfo;
import feast.core.service.JobManagementService;
import feast.core.service.SpecService;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

/**
 * Web service serving the feast UI.
 */
@CrossOrigin(maxAge = 3600)
@RestController
@Slf4j
public class UiServiceController {

  private final SpecService specService;
  private final JobManagementService jobManagementService;

  @Autowired
  public UiServiceController(SpecService specService, JobManagementService jobManagementService) {
    this.specService = specService;
    this.jobManagementService = jobManagementService;
  }

  /**
   * List all feature specs registered in the registry.
   */
  @RequestMapping(
      value = "/api/ui/features",
      produces = "application/json",
      method = RequestMethod.GET)
  public ListFeaturesResponse listFeatures() {
    try {
      List<FeatureDetail> features =
          specService
              .listFeatures()
              .stream()
              .map((fi) -> fi
                  .getFeatureDetail(specService.getStorageSpecs()))
              .collect(Collectors.toList());
      return ListFeaturesResponse.newBuilder().addAllFeatures(features).build();
    } catch (Exception e) {
      log.error("Exception in listFeatures: {}", e);
      throw e;
    }
  }

  /**
   * Get a single feature spec by ID.
   */
  @RequestMapping(
      value = "/api/ui/features/{id}",
      produces = "application/json",
      method = RequestMethod.GET)
  public GetFeatureResponse getFeature(@PathVariable("id") String id) {
    try {
      FeatureInfo featureInfo = specService.getFeatures(Arrays.asList(id)).get(0);
      FeatureInfo resolved = featureInfo.resolve();
      return GetFeatureResponse.newBuilder()
          .setFeature(resolved.getFeatureDetail(specService.getStorageSpecs()))
          .setRawSpec(featureInfo.getFeatureSpec())
          .build();
    } catch (Exception e) {
      log.error("Exception in getFeature {}: {}", id, e);
      throw e;
    }
  }

  /**
   * List all feature group specs registered in the registry.
   */
  @RequestMapping(
      value = "/api/ui/feature_groups",
      produces = "application/json",
      method = RequestMethod.GET)
  public ListFeatureGroupsResponse listFeatureGroups() {
    try {
      List<FeatureGroupDetail> featureGroups =
          specService
              .listFeatureGroups()
              .stream()
              .map(FeatureGroupInfo::getFeatureGroupDetail)
              .collect(Collectors.toList());
      return ListFeatureGroupsResponse.newBuilder().addAllFeatureGroups(featureGroups).build();
    } catch (Exception e) {
      log.error("Exception in listFeatureGroups: {}", e);
      throw e;
    }
  }

  /**
   * Get a single feature group spec by ID.
   */
  @RequestMapping(
      value = "/api/ui/feature_groups/{id}",
      produces = "application/json",
      method = RequestMethod.GET)
  public GetFeatureGroupResponse getFeatureGroup(@PathVariable("id") String id) {
    try {
      FeatureGroupInfo featureGroupInfo = specService.getFeatureGroups(Arrays.asList(id)).get(0);
      return GetFeatureGroupResponse.newBuilder()
          .setFeatureGroup(featureGroupInfo.getFeatureGroupDetail())
          .build();
    } catch (Exception e) {
      log.error("Exception in getFeatureGroup {}: {}", id, e);
      throw e;
    }
  }

  /**
   * List all entity specs registered in the registry.
   */
  @RequestMapping(
      value = "/api/ui/entities",
      produces = "application/json",
      method = RequestMethod.GET)
  public ListEntitiesResponse listEntities() {
    try {
      List<EntityDetail> entities =
          specService
              .listEntities()
              .stream()
              .map(EntityInfo::getEntityDetail)
              .collect(Collectors.toList());
      return ListEntitiesResponse.newBuilder().addAllEntities(entities).build();
    } catch (Exception e) {
      log.error("Exception in listEntities: {}", e);
      throw e;
    }
  }

  /**
   * Get a single entity spec by name.
   */
  @RequestMapping(
      value = "/api/ui/entities/{id}",
      produces = "application/json",
      method = RequestMethod.GET)
  public GetEntityResponse getEntity(@PathVariable("id") String id) {
    try {
      EntityInfo entityInfo = specService.getEntities(Arrays.asList(id)).get(0);
      return GetEntityResponse.newBuilder().setEntity(entityInfo.getEntityDetail()).build();
    } catch (Exception e) {
      log.error("Exception in getEntity {}: {}", id, e);
      throw e;
    }
  }

  /**
   * List all storage specs registered in the registry.
   */
  @RequestMapping(
      value = "/api/ui/storage",
      produces = "application/json",
      method = RequestMethod.GET)
  public ListStorageResponse listStorage() {
    try {
      List<StorageDetail> storage =
          specService
              .listStorage()
              .stream()
              .map(StorageInfo::getStorageDetail)
              .collect(Collectors.toList());
      return ListStorageResponse.newBuilder().addAllStorage(storage).build();
    } catch (Exception e) {
      log.error("Exception in listStorage: {}", e);
      throw e;
    }
  }

  /**
   * Get a single storage spec by name.
   */
  @RequestMapping(
      value = "/api/ui/storage/{id}",
      produces = "application/json",
      method = RequestMethod.GET)
  public GetStorageResponse getStorage(@PathVariable("id") String id) {
    try {
      StorageInfo storageInfo = specService.getStorage(Arrays.asList(id)).get(0);
      return GetStorageResponse.newBuilder().setStorage(storageInfo.getStorageDetail()).build();
    } catch (Exception e) {
      log.error("Exception in getStorage {}: {}", id, e);
      throw e;
    }
  }

  @RequestMapping(value = "/api/ui/jobs", produces = "application/json", method = RequestMethod.GET)
  public ListJobsResponse listJobs() {
    try {
      return ListJobsResponse.newBuilder().addAllJobs(jobManagementService.listJobs()).build();
    } catch (Exception e) {
      log.error("Exception in listJobs: {}", e);
      throw e;
    }
  }

  /**
   * Get a single job by id.
   */
  @RequestMapping(
      value = "/api/ui/jobs/{id}",
      produces = "application/json",
      method = RequestMethod.GET)
  public GetJobResponse getJob(@PathVariable("id") String id) {
    try {
      return GetJobResponse.newBuilder().setJob(jobManagementService.getJob(id)).build();
    } catch (Exception e) {
      log.error("Exception in getJob {}: {}", id, e);
      throw e;
    }
  }
}
