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
import feast.core.service.ProjectService;
import feast.core.service.SpecService;
import feast.core.service.StatsService;
import feast.proto.core.CoreServiceProto.GetFeastCoreVersionResponse;
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
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.CrossOrigin;
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
  private StatsService statsService;
  private ProjectService projectService;

  @Autowired
  public CoreServiceRestController(
      FeastProperties feastProperties,
      SpecService specService,
      StatsService statsService,
      ProjectService projectService) {
    this.feastProperties = feastProperties;
    this.specService = specService;
    this.statsService = statsService;
    this.projectService = projectService;
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
   * GET /feature-sets : Retrieve a list of Feature Sets according to filtering parameters of Feast
   * project name and feature set name. If none matches, an empty JSON response is returned.
   *
   * @param project Request Parameter: Name of feast project to search in. If set to <code>"*"
   *                </code>, all existing projects will be filtered. However, asterisk can NOT be
   *     combined with other strings (for example <code>"merchant_*"</code>) to use as wildcard to
   *     filter feature sets.
   * @param name Request Parameter: Feature set name. If set to "*", filter * all feature sets by
   *     default. Asterisk can be used as wildcard to filter * feature sets.
   * @return (200 OK) Return {@link ListFeatureSetsResponse} in JSON.
   */
  @RequestMapping(value = "/feature-sets", method = RequestMethod.GET)
  public ListFeatureSetsResponse listFeatureSets(
      @RequestParam(defaultValue = Project.DEFAULT_NAME) String project, @RequestParam String name)
      throws InvalidProtocolBufferException {
    ListFeatureSetsRequest.Filter.Builder filterBuilder =
        ListFeatureSetsRequest.Filter.newBuilder().setProject(project).setFeatureSetName(name);
    return specService.listFeatureSets(filterBuilder.build());
  }

  /**
   * GET /features : List Features based on project and entities.
   *
   * @param entities Request Parameter: List of all entities every returned feature should belong
   *     to. At least one entity is required. For example, if <code>entity1</code> and <code>entity2
   *                 </code> are given, then all features returned (if any) will belong to BOTH
   *     entities.
   * @param project (Optional) Request Parameter: A single project where the feature set of all
   *     features returned is under. If not provided, the default project will be used, usually
   *     <code>default</code>.
   * @return (200 OK) Return {@link ListFeaturesResponse} in JSON.
   */
  @RequestMapping(value = "/features", method = RequestMethod.GET)
  public ListFeaturesResponse listFeatures(
      @RequestParam String[] entities, @RequestParam(required = false) Optional<String> project) {
    ListFeaturesRequest.Filter.Builder filterBuilder =
        ListFeaturesRequest.Filter.newBuilder().addAllEntities(Arrays.asList(entities));
    project.ifPresent(filterBuilder::setProject);
    return specService.listFeatures(filterBuilder.build());
  }

  /**
   * GET /feature-statistics : Fetches statistics for a dataset speficied by the parameters. Either
   * both (start_date, end_date) need to be given or ingestion_ids are required. If both are given,
   * (start_date, end_date) will be ignored.
   *
   * @param ingestionIds Request Parameter: List of ingestion IDs. If missing, both startDate and
   *     endDate should be provided.
   * @param startDate Request Parameter: UTC+0 starting date (inclusive) in the ISO format, from
   *     <code>0001-01-01</code> to <code>9999-12-31</code>. Time given will be ignored. This
   *     parameter will be ignored if any ingestionIds is provided.
   * @param endDate Request Parameter: UTC+0 ending date (exclusive) in the ISO format, from <code>
   *      0001-01-01</code> to <code>9999-12-31</code>. Time given will be ignored. This parameter
   *     will be ignored if any ingestionIds is provided.
   * @param store Request Parameter: The name of the historical store used in Feast Serving. Online
   *     store is not allowed.
   * @param featureSetId Request Parameter: Feature set ID, which has the form of <code>
   *                     project/feature_set_name</code>.
   * @param forceRefresh Request Parameter: whether to override the values in the cache. Accepts
   *     <code>true</code>, <code>false</code>.
   * @param features (Optional) Request Parameter: List of features. If none provided, all features
   *     in the feature set will be used for statistics.
   * @return (200 OK) Returns {@link GetFeatureStatisticsResponse} in JSON.
   */
  @RequestMapping(value = "/feature-statistics", method = RequestMethod.GET)
  public GetFeatureStatisticsResponse getFeatureStatistics(
      @RequestParam(name = "feature_set_id") String featureSetId,
      @RequestParam(required = false) Optional<String[]> features,
      @RequestParam String store,
      @RequestParam(name = "start_date", required = false) Optional<String> startDate,
      @RequestParam(name = "end_date", required = false) Optional<String> endDate,
      @RequestParam(name = "ingestion_ids", required = false) Optional<String[]> ingestionIds,
      @RequestParam(name = "force_refresh") boolean forceRefresh)
      throws IOException {

    Builder requestBuilder =
        GetFeatureStatisticsRequest.newBuilder()
            .setForceRefresh(forceRefresh)
            .setFeatureSetId(featureSetId)
            .setStore(store);

    // set optional request parameters if they are provided
    features.ifPresent(theFeatures -> requestBuilder.addAllFeatures(Arrays.asList(theFeatures)));
    startDate.ifPresent(
        startDateStr -> requestBuilder.setStartDate(utcTimeStringToTimestamp(startDateStr)));
    endDate.ifPresent(
        endDateStr -> requestBuilder.setEndDate(utcTimeStringToTimestamp(endDateStr)));
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
    List<Project> projects = projectService.listProjects();
    return ListProjectsResponse.newBuilder()
        .addAllProjects(projects.stream().map(Project::getName).collect(Collectors.toList()))
        .build();
  }

  private Timestamp utcTimeStringToTimestamp(String utcTimeString) {
    long epochSecond =
        LocalDate.parse(utcTimeString, DateTimeFormatter.ISO_DATE)
            .toEpochSecond(LocalTime.MIN, ZoneOffset.UTC);
    return Timestamp.newBuilder().setSeconds(epochSecond).setNanos(0).build();
  }
}
