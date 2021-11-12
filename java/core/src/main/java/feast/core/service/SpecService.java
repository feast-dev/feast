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
import static feast.core.validators.Matchers.checkValidCharactersAllowDash;

import com.google.protobuf.InvalidProtocolBufferException;
import feast.core.dao.EntityRepository;
import feast.core.dao.FeatureTableRepository;
import feast.core.dao.ProjectRepository;
import feast.core.dao.StoreRepository;
import feast.core.exception.RetrievalException;
import feast.core.model.*;
import feast.core.validators.EntityValidator;
import feast.core.validators.FeatureTableValidator;
import feast.proto.core.CoreServiceProto.ApplyEntityResponse;
import feast.proto.core.CoreServiceProto.ApplyFeatureTableRequest;
import feast.proto.core.CoreServiceProto.ApplyFeatureTableResponse;
import feast.proto.core.CoreServiceProto.DeleteFeatureTableRequest;
import feast.proto.core.CoreServiceProto.GetEntityRequest;
import feast.proto.core.CoreServiceProto.GetEntityResponse;
import feast.proto.core.CoreServiceProto.GetFeatureTableRequest;
import feast.proto.core.CoreServiceProto.GetFeatureTableResponse;
import feast.proto.core.CoreServiceProto.ListEntitiesRequest;
import feast.proto.core.CoreServiceProto.ListEntitiesResponse;
import feast.proto.core.CoreServiceProto.ListFeatureTablesRequest;
import feast.proto.core.CoreServiceProto.ListFeatureTablesResponse;
import feast.proto.core.CoreServiceProto.ListFeaturesRequest;
import feast.proto.core.CoreServiceProto.ListFeaturesResponse;
import feast.proto.core.CoreServiceProto.ListStoresRequest;
import feast.proto.core.CoreServiceProto.ListStoresResponse;
import feast.proto.core.CoreServiceProto.ListStoresResponse.Builder;
import feast.proto.core.CoreServiceProto.UpdateStoreRequest;
import feast.proto.core.CoreServiceProto.UpdateStoreResponse;
import feast.proto.core.EntityProto;
import feast.proto.core.FeatureTableProto.FeatureTableSpec;
import feast.proto.core.StoreProto;
import feast.proto.core.StoreProto.Store.Subscription;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
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

  private final EntityRepository entityRepository;
  private final FeatureTableRepository tableRepository;
  private final ProjectRepository projectRepository;
  private final StoreRepository storeRepository;

  @Autowired
  public SpecService(
      EntityRepository entityRepository,
      FeatureTableRepository tableRepository,
      StoreRepository storeRepository,
      ProjectRepository projectRepository) {
    this.entityRepository = entityRepository;
    this.tableRepository = tableRepository;
    this.storeRepository = storeRepository;
    this.projectRepository = projectRepository;
  }

  /**
   * Get an entity matching the entity name and set project. The entity name and project are
   * required. If the project is omitted, the default would be used.
   *
   * @param request GetEntityRequest Request
   * @return Returns a GetEntityResponse containing an entity
   */
  public GetEntityResponse getEntity(GetEntityRequest request) {
    String projectName = request.getProject();
    String entityName = request.getName();

    if (entityName.isEmpty()) {
      throw new IllegalArgumentException("No entity name provided");
    }
    // Autofill default project if project is not specified
    if (projectName.isEmpty()) {
      projectName = Project.DEFAULT_NAME;
    }

    checkValidCharactersAllowDash(projectName, "project");
    checkValidCharacters(entityName, "entity");

    EntityV2 entity = entityRepository.findEntityByNameAndProject_Name(entityName, projectName);

    if (entity == null) {
      throw new RetrievalException(
          String.format("Entity with name \"%s\" could not be found.", entityName));
    }

    // Build GetEntityResponse
    GetEntityResponse response = GetEntityResponse.newBuilder().setEntity(entity.toProto()).build();

    return response;
  }

  /**
   * Return a map of feature references and features matching the project, labels and entities
   * provided in the filter. All fields are required.
   *
   * <p>Project name must be explicitly provided or if the project name is omitted, the default
   * project would be used. A combination of asterisks/wildcards and text is not allowed.
   *
   * <p>The entities in the filter accepts a list. All matching features will be returned. Regex is
   * not supported. If no entities are provided, features will not be filtered by entities.
   *
   * <p>The labels in the filter accepts a map. All matching features will be returned. Regex is not
   * supported. If no labels are provided, features will not be filtered by labels.
   *
   * @param filter filter containing the desired project name, entities and labels
   * @return ListEntitiesResponse with map of feature references and features found matching the
   *     filter
   */
  public ListFeaturesResponse listFeatures(ListFeaturesRequest.Filter filter) {
    String project = filter.getProject();
    List<String> entities = filter.getEntitiesList();
    Map<String, String> labels = filter.getLabelsMap();

    // Autofill default project if project not specified
    if (project.isEmpty()) {
      project = Project.DEFAULT_NAME;
    }

    checkValidCharactersAllowDash(project, "project");

    // Currently defaults to all FeatureTables
    List<FeatureTable> featureTables = tableRepository.findAllByProject_Name(project);

    ListFeaturesResponse.Builder response = ListFeaturesResponse.newBuilder();
    if (entities.size() > 0) {
      featureTables =
          featureTables.stream()
              .filter(featureTable -> featureTable.hasAllEntities(entities))
              .collect(Collectors.toList());
    }

    Map<String, FeatureV2> featuresMap;
    for (FeatureTable featureTable : featureTables) {
      featuresMap = featureTable.getFeaturesByLabels(labels);
      for (Map.Entry<String, FeatureV2> entry : featuresMap.entrySet()) {
        response.putFeatures(entry.getKey(), entry.getValue().toProto());
      }
    }

    return response.build();
  }

  /**
   * Return a list of entities matching the entity name, project and labels provided in the filter.
   * All fields are required. Use '*' in entity name and project, and empty map in labels in order
   * to return all entities in all projects.
   *
   * <p>Project name can be explicitly provided, or an asterisk can be provided to match all
   * projects. It is not possible to provide a combination of asterisks/wildcards and text. If the
   * project name is omitted, the default project would be used.
   *
   * <p>The entity name in the filter accepts an asterisk as a wildcard. All matching entities will
   * be returned. Regex is not supported. Explicitly defining an entity name is not possible if a
   * project name is not set explicitly.
   *
   * <p>The labels in the filter accepts a map. All entities which contain every provided label will
   * be returned.
   *
   * @param filter Filter containing the desired entity name, project and labels
   * @return ListEntitiesResponse with list of entities found matching the filter
   */
  public ListEntitiesResponse listEntities(ListEntitiesRequest.Filter filter) {
    String project = filter.getProject();
    Map<String, String> labelsFilter = filter.getLabelsMap();

    // Autofill default project if project not specified
    if (project.isEmpty()) {
      project = Project.DEFAULT_NAME;
    }

    checkValidCharactersAllowDash(project, "project");

    List<EntityV2> entities = entityRepository.findAllByProject_Name(project);

    ListEntitiesResponse.Builder response = ListEntitiesResponse.newBuilder();
    if (entities.size() > 0) {
      entities =
          entities.stream()
              .filter(entity -> entity.hasAllLabels(labelsFilter))
              .collect(Collectors.toList());
      for (EntityV2 entity : entities) {
        response.addEntities(entity.toProto());
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
   * Creates or updates an entity in the repository.
   *
   * <p>This function is idempotent. If no changes are detected in the incoming entity's schema,
   * this method will return the existing entity stored in the repository. If project is not
   * specified, the entity will be assigned to the 'default' project.
   *
   * @param newEntitySpec EntitySpecV2 that will be used to create or update an Entity.
   * @param projectName Project namespace of Entity which is to be created/updated
   * @return response of the operation
   */
  @Transactional
  public ApplyEntityResponse applyEntity(
      EntityProto.EntitySpecV2 newEntitySpec, String projectName) {
    // Autofill default project if not specified
    if (projectName == null || projectName.isEmpty()) {
      projectName = Project.DEFAULT_NAME;
    }

    checkValidCharactersAllowDash(projectName, "project");

    // Validate incoming entity
    EntityValidator.validateSpec(newEntitySpec);

    // Find project or create new one if it does not exist
    Project project = projectRepository.findById(projectName).orElse(new Project(projectName));

    // Ensure that the project retrieved from repository is not archived
    if (project.isArchived()) {
      throw new IllegalArgumentException(String.format("Project is archived: %s", projectName));
    }

    // Retrieve existing Entity
    EntityV2 entity =
        entityRepository.findEntityByNameAndProject_Name(newEntitySpec.getName(), projectName);

    EntityProto.Entity newEntity = EntityProto.Entity.newBuilder().setSpec(newEntitySpec).build();
    if (entity == null) {
      // Create new entity since it doesn't exist
      entity = EntityV2.fromProto(newEntity);
    } else {
      // If the entity remains unchanged, we do nothing.
      if (entity.toProto().getSpec().equals(newEntitySpec)) {
        return ApplyEntityResponse.newBuilder().setEntity(entity.toProto()).build();
      }
      entity.updateFromProto(newEntity, projectName);
    }

    // Persist the EntityV2 object
    project.addEntity(entity);
    projectRepository.saveAndFlush(project);

    // Build ApplyEntityResponse
    ApplyEntityResponse response =
        ApplyEntityResponse.newBuilder().setEntity(entity.toProto()).build();
    return response;
  }

  /**
   * Resolves the project name by returning name if given, autofilling default project otherwise.
   *
   * @param projectName name of the project to resolve.
   * @return project name
   */
  public static String resolveProjectName(String projectName) {
    return (projectName.isEmpty()) ? Project.DEFAULT_NAME : projectName;
  }

  /**
   * UpdateStore updates the repository with the new given store.
   *
   * @param updateStoreRequest containing the new store definition
   * @return UpdateStoreResponse containing the new store definition
   * @throws InvalidProtocolBufferException if protobuf exception occurs
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
   * Applies the given FeatureTable to the FeatureTable registry. Creates the FeatureTable if does
   * not exist, otherwise updates the existing FeatureTable. Applies FeatureTable in project if
   * specified, otherwise in default project.
   *
   * @param request Contains FeatureTable spec and project parameters used to create or update a
   *     FeatureTable.
   * @throws NoSuchElementException projects and entities referenced in request do not exist.
   * @return response containing the applied FeatureTable spec.
   */
  @Transactional
  public ApplyFeatureTableResponse applyFeatureTable(ApplyFeatureTableRequest request) {
    String projectName = resolveProjectName(request.getProject());

    checkValidCharactersAllowDash(projectName, "project");

    // Check that specification provided is valid
    FeatureTableSpec applySpec = request.getTableSpec();
    FeatureTableValidator.validateSpec(applySpec);

    // Prevent apply if the project is archived.
    Project project = projectRepository.findById(projectName).orElse(new Project(projectName));
    if (project.isArchived()) {
      throw new IllegalArgumentException(
          String.format(
              "Cannot apply Feature Table to archived Project: (table: %s, project: %s)",
              applySpec.getName(), projectName));
    }

    // Create or update depending on whether there is an existing Feature Table
    Optional<FeatureTable> existingTable =
        tableRepository.findFeatureTableByNameAndProject_Name(applySpec.getName(), projectName);
    FeatureTable table = FeatureTable.fromProto(projectName, applySpec, entityRepository);
    if (existingTable.isPresent() && table.equals(existingTable.get())) {
      // Skip update if no change is detected
      return ApplyFeatureTableResponse.newBuilder().setTable(existingTable.get().toProto()).build();
    }
    if (existingTable.isPresent()) {
      existingTable.get().updateFromProto(projectName, applySpec, entityRepository);
      table = existingTable.get();
    }

    // Commit FeatureTable to database and return applied FeatureTable
    tableRepository.saveAndFlush(table);
    return ApplyFeatureTableResponse.newBuilder().setTable(table.toProto()).build();
  }

  /**
   * List the FeatureTables matching the filter in the given filter. Scopes down listing to project
   * if specified, the default project otherwise.
   *
   * @param filter Filter containing the desired project and labels
   * @return ListFeatureTablesResponse with list of FeatureTables found matching the filter
   */
  @Transactional
  public ListFeatureTablesResponse listFeatureTables(ListFeatureTablesRequest.Filter filter) {
    String projectName = resolveProjectName(filter.getProject());
    Map<String, String> labelsFilter = filter.getLabelsMap();

    checkValidCharactersAllowDash(projectName, "project");

    List<FeatureTable> matchingTables = tableRepository.findAllByProject_Name(projectName);

    ListFeatureTablesResponse.Builder response = ListFeatureTablesResponse.newBuilder();

    if (matchingTables.size() > 0) {
      matchingTables =
          matchingTables.stream()
              .filter(table -> table.hasAllLabels(labelsFilter))
              .filter(table -> !table.isDeleted())
              .collect(Collectors.toList());
    }
    for (FeatureTable table : matchingTables) {
      response.addTables(table.toProto());
    }

    return response.build();
  }

  /**
   * Get the FeatureTable with the name and project specified in the request. Gets FeatureTable in
   * project if specified, otherwise in default project.
   *
   * @param request containing the retrieval parameters.
   * @throws NoSuchElementException if no FeatureTable matches given request.
   * @return response containing the requested FeatureTable.
   */
  @Transactional
  public GetFeatureTableResponse getFeatureTable(GetFeatureTableRequest request) {
    String projectName = resolveProjectName(request.getProject());
    String featureTableName = request.getName();

    checkValidCharactersAllowDash(projectName, "project");
    checkValidCharacters(featureTableName, "featureTable");

    Optional<FeatureTable> retrieveTable =
        tableRepository.findFeatureTableByNameAndProject_Name(featureTableName, projectName);
    if (retrieveTable.isEmpty()) {
      throw new NoSuchElementException(
          String.format(
              "No such Feature Table: (project: %s, name: %s)", projectName, featureTableName));
    }

    if (retrieveTable.get().isDeleted()) {
      throw new NoSuchElementException(
          String.format(
              "Feature Table has been deleted: (project: %s, name: %s)",
              projectName, featureTableName));
    }

    // Build GetFeatureTableResponse
    GetFeatureTableResponse response =
        GetFeatureTableResponse.newBuilder().setTable(retrieveTable.get().toProto()).build();

    return response;
  }

  @Transactional
  public void deleteFeatureTable(DeleteFeatureTableRequest request) {
    String projectName = resolveProjectName(request.getProject());
    String featureTableName = request.getName();

    checkValidCharactersAllowDash(projectName, "project");
    checkValidCharacters(featureTableName, "featureTable");

    Optional<FeatureTable> existingTable =
        tableRepository.findFeatureTableByNameAndProject_Name(featureTableName, projectName);
    if (existingTable.isEmpty()) {
      throw new NoSuchElementException(
          String.format(
              "No such Feature Table: (project: %s, name: %s)", projectName, featureTableName));
    }

    existingTable.get().delete();
  }
}
