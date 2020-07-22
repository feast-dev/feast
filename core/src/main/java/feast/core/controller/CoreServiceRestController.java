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
package feast.core.controller;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Timestamp;
import feast.core.config.FeastProperties;
import feast.core.model.Project;
import feast.core.service.AccessManagementService;
import feast.core.service.JobService;
import feast.core.service.SpecService;
import feast.core.service.StatsService;
import feast.proto.core.CoreServiceProto.GetFeastCoreVersionResponse;
import feast.proto.core.CoreServiceProto.GetFeatureSetRequest;
import feast.proto.core.CoreServiceProto.GetFeatureSetResponse;
import feast.proto.core.CoreServiceProto.GetFeatureStatisticsRequest;
import feast.proto.core.CoreServiceProto.GetFeatureStatisticsRequest.Builder;
import feast.proto.core.CoreServiceProto.GetFeatureStatisticsResponse;
import feast.proto.core.CoreServiceProto.ListFeatureSetsRequest;
import feast.proto.core.CoreServiceProto.ListFeatureSetsResponse;
import feast.proto.core.CoreServiceProto.ListFeaturesRequest;
import feast.proto.core.CoreServiceProto.ListFeaturesResponse;
import feast.proto.core.CoreServiceProto.ListProjectsResponse;
import java.io.IOException;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * EXPERIMENTAL: Controller for HTTP endpoints to Feast Core. These endpoints are subject to change.
 */
@RestController
@CrossOrigin
@Slf4j
@RequestMapping(value = "/api/v1", produces = "application/json")
public class CoreServiceRestController {

  private final FeastProperties feastProperties;
  private SpecService specService;
  private JobService jobService;
  private StatsService statsService;
  private AccessManagementService accessManagementService;

  @Autowired
  public CoreServiceRestController(
      FeastProperties feastProperties,
      SpecService specService,
      JobService jobService,
      StatsService statsService,
      AccessManagementService accessManagementService) {
    this.feastProperties = feastProperties;
    this.specService = specService;
    this.jobService = jobService;
    this.statsService = statsService;
    this.accessManagementService = accessManagementService;
  }

  /**
   * GET /version : Fetches the version of Feast Core.
   *
   * @return (200 OK) Returns {@link GetFeastCoreVersionResponse} in JSON.
   */
  @RequestMapping(value = "/version", method = RequestMethod.GET)
  public GetFeastCoreVersionResponse getVersion() {
    GetFeastCoreVersionResponse response =
        GetFeastCoreVersionResponse.newBuilder().setVersion(feastProperties.getVersion()).build();
    return response;
  }

  /**
   * GET /project/{project}/feature-set/{featureSetName} : Get information about a feature set in a
   * project. To get multiple feature sets, use the (GET /feature-sets) endpoint.
   *
   * @param project Feast project name.
   * @param featureSetName Feast feature set name.
   * @return (200 OK) Returns {@link GetFeatureSetResponse} in JSON.
   * @throws InvalidProtocolBufferException (500 Internal Server Error)
   */
  @RequestMapping(
      value = "/project/{project}/feature-set/{featureSetName}",
      method = RequestMethod.GET)
  public GetFeatureSetResponse getFeatureSet(
      @PathVariable String project, @PathVariable String featureSetName)
      throws InvalidProtocolBufferException {
    GetFeatureSetRequest request =
        GetFeatureSetRequest.newBuilder().setProject(project).setName(featureSetName).build();
    return specService.getFeatureSet(request);
  }

  /**
   * GET /feature-sets : Retrieve a list of Feature Sets according to filtering parameters of Feast
   * project, feature set name and labels. If none match, an empty JSON response is returned.
   *
   * @param project (Optional) Request Parameter: Name of feast project to search in. If absent or
   *     set to "*", filter all projects by default. Asterisk can NOT be used as wildcard to filter
   *     projects.
   * @param name (Optional) Request Parameter: Feature set name. If absent or set to "*", filter *
   *     all feature sets by default. Asterisk can be used as wildcard to filter * feature sets.
   * @param labels (Optional) Request Parameter: Key-value pair of feature set labels to filter
   *     results.
   * @return (200 OK) Return {@link ListFeatureSetsResponse} in JSON.
   */
  @RequestMapping(value = "/feature-sets", method = RequestMethod.GET)
  public ListFeatureSetsResponse listFeatureSets(
      @RequestParam(defaultValue = "*") String project,
      @RequestParam(defaultValue = "*") String name,
      @RequestParam(required = false) Optional<Map<String, String>> labels)
      throws InvalidProtocolBufferException {
    ListFeatureSetsRequest.Filter.Builder filterBuilder =
        ListFeatureSetsRequest.Filter.newBuilder().setProject(project).setFeatureSetName(name);
    labels.ifPresent(filterBuilder::putAllLabels);
    return specService.listFeatureSets(filterBuilder.build());
  }

  /**
   * GET /features : List Features based on project and entities.
   *
   * @param entities List of all entities every returned feature should belong to. At least one
   *     entity is required. For example, if <code>entity1</code> and <code>entity2</code> are
   *     given, then all features returned (if any) will belong to BOTH entities.
   * @param labels (Optional) Key-value pair of labels. Only features with ALL matching labels will
   *     be returned.
   * @param project (Optional) A single project where the feature set of all features returned is
   *     under.
   * @return (200 OK) Return {@link ListFeaturesResponse} in JSON.
   */
  @RequestMapping(value = "/features", method = RequestMethod.GET)
  public ListFeaturesResponse listFeatures(
      @RequestParam String[] entities,
      @RequestParam Optional<Map<String, String>> labels,
      @RequestParam(defaultValue = Project.DEFAULT_NAME) String project) {
    ListFeaturesRequest.Filter.Builder filterBuilder =
        ListFeaturesRequest.Filter.newBuilder()
            .setProject(project)
            .addAllEntities(Arrays.asList(entities));
    labels.ifPresent(filterBuilder::putAllLabels);
    return specService.listFeatures(filterBuilder.build());
  }

  /**
   * GET /feature-statistics : Fetches statistics for a dataset speficied by the parameters. Either
   * both (start_date, end_date) need to be given or ingestion_ids are required.
   *
   * @param ingestionIds Request Parameter: List of ingestion IDs. If missing, both startDate and
   *     endDate should be provided.
   * @param startDate Request Parameter: UTC+0 starting date (inclusive) in the ISO format, from
   *     <code>0001-01-01T00:00:00Z</code> to <code>9999-12-31T23:59:59.999999999Z</code>. Time
   *     given will be ignored.
   * @param endDate Request Parameter: UTC+0 ending date (exclusive) in the ISO format, from <code>
   *     0001-01-01T00:00:00Z</code> to <code>9999-12-31T23:59:59.999999999Z</code>. Time given will
   *     be ignored.
   * @param featureSetId (Optional) Request Parameter: Feature set ID, which has the form of <code>
   *     project/feature_set_name</code>.
   * @param features (Optional) Request Parameter: List of features.
   * @param store (Optional) Request Parameter:
   * @param forceRefresh (Optional) Request Parameter: whether to override the values in the cache.
   *     Accepts <code>true</code>, <code>false</code>.
   * @return (200 OK) Returns {@link GetFeatureStatisticsResponse} in JSON.
   */
  @RequestMapping(value = "/feature-statistics", method = RequestMethod.GET)
  public GetFeatureStatisticsResponse getFeatureStatistics(
      @RequestParam(name = "feature_set_id", required = false) Optional<String> featureSetId,
      @RequestParam(required = false) Optional<String[]> features,
      @RequestParam(required = false) Optional<String> store,
      @RequestParam(name = "start_date", required = false) Optional<String> startDate,
      @RequestParam(name = "end_date", required = false) Optional<String> endDate,
      @RequestParam(name = "ingestion_ids", required = false) Optional<String[]> ingestionIds,
      @RequestParam(name = "force_refresh", defaultValue = "false") boolean forceRefresh)
      throws IOException {

    Builder requestBuilder = GetFeatureStatisticsRequest.newBuilder().setForceRefresh(forceRefresh);

    // set optional request parameters if they are provided
    featureSetId.ifPresent(requestBuilder::setFeatureSetId);
    store.ifPresent(requestBuilder::setStore);
    features.ifPresent(theFeatures -> requestBuilder.addAllFeatures(Arrays.asList(theFeatures)));
    startDate.ifPresent(
        startDateStr -> requestBuilder.setStartDate(UtcTimeStringToTimestamp(startDateStr)));
    endDate.ifPresent(
        endDateStr -> requestBuilder.setStartDate(UtcTimeStringToTimestamp(endDateStr)));
    ingestionIds.ifPresent(
        theIngestionIds -> requestBuilder.addAllIngestionIds(Arrays.asList(theIngestionIds)));

    return statsService.getFeatureStatistics(requestBuilder.build());
  }

  /**
   * GET /projects : Get the list of existing feast projects.
   *
   * @return (200 OK) Returns {@link ListProjectsResponse} in JSON.
   */
  @RequestMapping(value = "/projects", method = RequestMethod.GET)
  public ListProjectsResponse listProjects() {
    List<Project> projects = accessManagementService.listProjects();
    return ListProjectsResponse.newBuilder()
        .addAllProjects(projects.stream().map(Project::getName).collect(Collectors.toList()))
        .build();
  }

  private Timestamp UtcTimeStringToTimestamp(String utcTimeString) {
    long epochSecond = LocalDate.parse(utcTimeString).toEpochSecond(LocalTime.MIN, ZoneOffset.UTC);
    return Timestamp.newBuilder().setSeconds(epochSecond).setNanos(0).build();
  }
}
