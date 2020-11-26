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

import feast.core.config.FeastProperties;
import feast.core.model.Project;
import feast.core.service.ProjectService;
import feast.core.service.SpecService;
import feast.proto.core.CoreServiceProto.GetFeastCoreVersionResponse;
import feast.proto.core.CoreServiceProto.ListEntitiesRequest;
import feast.proto.core.CoreServiceProto.ListEntitiesResponse;
import feast.proto.core.CoreServiceProto.ListFeatureTablesRequest;
import feast.proto.core.CoreServiceProto.ListFeatureTablesResponse;
import feast.proto.core.CoreServiceProto.ListFeaturesRequest;
import feast.proto.core.CoreServiceProto.ListFeaturesResponse;
import feast.proto.core.CoreServiceProto.ListProjectsResponse;
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
@RequestMapping(value = "/api", produces = "application/json")
public class CoreServiceRestController {

  private final FeastProperties feastProperties;
  private SpecService specService;
  private ProjectService projectService;

  @Autowired
  public CoreServiceRestController(
      FeastProperties feastProperties, SpecService specService, ProjectService projectService) {
    this.feastProperties = feastProperties;
    this.specService = specService;
    this.projectService = projectService;
  }

  /**
   * GET /version : Fetches the version of Feast Core.
   *
   * @return (200 OK) Returns {@link GetFeastCoreVersionResponse} in JSON.
   */
  @RequestMapping(value = "/v2/version", method = RequestMethod.GET)
  public GetFeastCoreVersionResponse getVersion() {
    GetFeastCoreVersionResponse response =
        GetFeastCoreVersionResponse.newBuilder().setVersion(feastProperties.getVersion()).build();
    return response;
  }

  /**
   * GET /features : List Features based on project and entities.
   *
   * @param entities Request Parameter: List of all entities every returned feature should belong
   *     to. At least one entity is required. For example, if <code>entity1</code> and <code>entity2
   *                 </code> are given, then all features returned (if any) will belong to BOTH
   *     entities.
   * @param project (Optional) Request Parameter: A single project where the feature table of all
   *     features returned is under. If not provided, the default project will be used, usually
   *     <code>default</code>.
   * @return (200 OK) Return {@link ListFeaturesResponse} in JSON.
   */
  @RequestMapping(value = "/v2/features", method = RequestMethod.GET)
  public ListFeaturesResponse listFeatures(
      @RequestParam String[] entities, @RequestParam(required = false) Optional<String> project) {
    ListFeaturesRequest.Filter.Builder filterBuilder =
        ListFeaturesRequest.Filter.newBuilder().addAllEntities(Arrays.asList(entities));
    project.ifPresent(filterBuilder::setProject);
    return specService.listFeatures(filterBuilder.build());
  }

  /**
   * GET /projects : Get the list of existing feast projects.
   *
   * @return (200 OK) Returns {@link ListProjectsResponse} in JSON.
   */
  @RequestMapping(value = "/v2/projects", method = RequestMethod.GET)
  public ListProjectsResponse listProjects() {
    List<Project> projects = projectService.listProjects();
    return ListProjectsResponse.newBuilder()
        .addAllProjects(projects.stream().map(Project::getName).collect(Collectors.toList()))
        .build();
  }

  /**
   * GET /entities : Retrieve a list of Entities according to filtering parameters of Feast project
   * name. If none matches, an empty JSON response is returned.
   *
   * @param project Request Parameter: Name of feast project to search in.
   * @return (200 OK) Return {@link ListEntitiesResponse} in JSON.
   */
  @RequestMapping(value = "/v2/entities", method = RequestMethod.GET)
  public ListEntitiesResponse listEntities(
      @RequestParam(defaultValue = Project.DEFAULT_NAME) String project) {
    ListEntitiesRequest.Filter.Builder filterBuilder =
        ListEntitiesRequest.Filter.newBuilder().setProject(project);
    return specService.listEntities(filterBuilder.build());
  }

  /**
   * GET /feature-tables : Retrieve a list of Feature Tables according to filtering parameters of
   * Feast project name. If none matches, an empty JSON response is returned.
   *
   * @param project Request Parameter: Name of feast project to search in.
   * @return (200 OK) Return {@link ListFeatureTablesResponse} in JSON.
   */
  @RequestMapping(value = "/v2/feature-tables", method = RequestMethod.GET)
  public ListFeatureTablesResponse listFeatureTables(
      @RequestParam(defaultValue = Project.DEFAULT_NAME) String project) {
    ListFeatureTablesRequest.Filter.Builder filterBuilder =
        ListFeatureTablesRequest.Filter.newBuilder().setProject(project);
    return specService.listFeatureTables(filterBuilder.build());
  }
}
