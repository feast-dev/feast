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
import com.google.protobuf.util.JsonFormat;
import feast.core.config.FeastProperties;
import feast.core.model.Project;
import feast.core.service.AccessManagementService;
import feast.core.service.JobService;
import feast.core.service.SpecService;
import feast.core.service.StatsService;
import feast.proto.core.CoreServiceProto.GetFeastCoreVersionResponse;
import feast.proto.core.CoreServiceProto.GetFeatureSetRequest;
import feast.proto.core.CoreServiceProto.GetFeatureStatisticsRequest;
import feast.proto.core.CoreServiceProto.GetFeatureStatisticsRequest.Builder;
import feast.proto.core.CoreServiceProto.ListFeatureSetsRequest;
import feast.proto.core.CoreServiceProto.ListFeaturesRequest;
import feast.proto.core.CoreServiceProto.ListProjectsResponse;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
@CrossOrigin
@RequestMapping(value = "/api/v1", produces = "application/json")
public class CoreServiceRestController {

  private final JsonFormat.Printer jsonPrinter;
  private final FeastProperties feastProperties;
  private SpecService specService;
  private JobService jobService;
  private StatsService statsService;
  private AccessManagementService accessManagementService;

  @Autowired
  public CoreServiceRestController(
      FeastProperties feastProperties, SpecService specService,
      JobService jobService, StatsService statsService,
      AccessManagementService accessManagementService) {
    this.feastProperties = feastProperties;
    this.specService = specService;
    this.jobService = jobService;
    this.statsService = statsService;
    this.accessManagementService = accessManagementService;
    this.jsonPrinter = JsonFormat.printer();
  }

  @RequestMapping(value = "/version", method = RequestMethod.GET)
  @ResponseBody
  public String getVersion() throws InvalidProtocolBufferException {
    GetFeastCoreVersionResponse response = GetFeastCoreVersionResponse.newBuilder()
        .setVersion(feastProperties.getVersion()).build();
    return jsonPrinter.print(response);
  }

  @RequestMapping(
      value = "/project/{project}/feature-set/{featureSetName}", method = RequestMethod.GET)
  @ResponseBody
  public String getFeatureSet(
      @PathVariable String project, @PathVariable String featureSetName)
      throws InvalidProtocolBufferException {
    GetFeatureSetRequest request =
        GetFeatureSetRequest.newBuilder().setProject(project).setName(featureSetName).build();
    return jsonPrinter.print(specService.getFeatureSet(request));
  }

  @RequestMapping(value = "/feature-sets", method = RequestMethod.GET)
  @ResponseBody
  public String listFeatureSets(@RequestParam String project,
      @RequestParam(defaultValue = "*") String name)
      throws InvalidProtocolBufferException {
    ListFeatureSetsRequest.Filter filter =
        ListFeatureSetsRequest.Filter.newBuilder()
            .setProject(project)
            .setFeatureSetName(name)
            .build();
    return jsonPrinter.print(specService.listFeatureSets(filter));
  }

  @RequestMapping(value = "/features", method = RequestMethod.GET)
  @ResponseBody
  public String listFeatures(@RequestParam String[] entities,
      @RequestParam(defaultValue = "default") String project)
      throws InvalidProtocolBufferException {
    ListFeaturesRequest.Filter filter =
        ListFeaturesRequest.Filter.newBuilder().setProject(project).addAllEntities(
            Arrays.asList(entities)).build();
    return jsonPrinter.print(specService.listFeatures(filter));
  }

  @RequestMapping(value = "/feature-statistics", method = RequestMethod.GET)
  @ResponseBody
  public String getFeatureStatistics(
      @RequestParam String feature_set_id,
      @RequestParam(required = false) Optional<String[]> features,
      @RequestParam(required = false) Optional<String> store,
      @RequestParam(name = "start_date", required = false) Optional<Timestamp> startDate,
      @RequestParam(name = "end_date", required = false) Optional<Timestamp> endDate,
      @RequestParam(required = false) Optional<String[]> ingestion_ids,
      @RequestParam(defaultValue = "false") boolean force_refresh)
      throws IOException {

    Builder requestBuilder = GetFeatureStatisticsRequest.newBuilder()
        .setFeatureSetId(feature_set_id)
        .setForceRefresh(force_refresh);

    // optional request parameters
    store.ifPresent(requestBuilder::setStore);
    features.ifPresent(theFeatures -> requestBuilder.addAllFeatures(Arrays.asList(theFeatures)));
    startDate.ifPresent(requestBuilder::setStartDate);
    endDate.ifPresent(requestBuilder::setEndDate);
    ingestion_ids.ifPresent(
        theIngestionIds -> requestBuilder.addAllIngestionIds(Arrays.asList(theIngestionIds)));

    return jsonPrinter.print(statsService.getFeatureStatistics(requestBuilder.build()));
  }

  @RequestMapping(value = "/projects", method = RequestMethod.GET)
  @ResponseBody
  public String listProjects() throws InvalidProtocolBufferException {
    List<Project> projects = accessManagementService.listProjects();
    ListProjectsResponse response = ListProjectsResponse.newBuilder()
        .addAllProjects(projects.stream().map(Project::getName).collect(Collectors.toList()))
        .build();
    return jsonPrinter.print(response);
  }
}
