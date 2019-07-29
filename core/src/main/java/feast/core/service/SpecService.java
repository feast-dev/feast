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

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.protobuf.util.JsonFormat;
import feast.core.config.StorageConfig.StorageSpecs;
import feast.core.dao.EntityInfoRepository;
import feast.core.dao.FeatureGroupInfoRepository;
import feast.core.dao.FeatureInfoRepository;
import feast.core.exception.RegistrationException;
import feast.core.exception.RetrievalException;
import feast.core.exception.TopicExistsException;
import feast.core.log.Action;
import feast.core.log.AuditLogger;
import feast.core.log.Resource;
import feast.core.model.EntityInfo;
import feast.core.model.FeatureGroupInfo;
import feast.core.model.FeatureInfo;
import feast.core.model.FeatureStreamTopic;
import feast.core.model.StorageInfo;
import feast.core.storage.SchemaManager;
import feast.core.stream.FeatureStream;
import feast.specs.EntitySpecProto.EntitySpec;
import feast.specs.FeatureGroupSpecProto.FeatureGroupSpec;
import feast.specs.FeatureSpecProto.FeatureSpec;
import feast.specs.StorageSpecProto.StorageSpec;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import lombok.Getter;
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

  private final EntityInfoRepository entityInfoRepository;
  private final FeatureInfoRepository featureInfoRepository;
  private final FeatureGroupInfoRepository featureGroupInfoRepository;
  private final SchemaManager schemaManager;
  private final FeatureStreamService featureStreamService;
  private final JobCoordinatorService jobCoordinatorService;

  @Getter
  private final StorageSpecs storageSpecs;

  @Autowired
  public SpecService(
      EntityInfoRepository entityInfoRegistry,
      FeatureInfoRepository featureInfoRegistry,
      FeatureGroupInfoRepository featureGroupInfoRepository,
      FeatureStreamService featureStreamService,
      JobCoordinatorService jobCoordinatorService,
      SchemaManager schemaManager,
      StorageSpecs storageSpecs) {
    this.entityInfoRepository = entityInfoRegistry;
    this.featureInfoRepository = featureInfoRegistry;
    this.featureGroupInfoRepository = featureGroupInfoRepository;
    this.featureStreamService = featureStreamService;
    this.jobCoordinatorService = jobCoordinatorService;
    this.schemaManager = schemaManager;
    this.storageSpecs = storageSpecs;
  }

  /**
   * Retrieve a set of entity infos from the registry.
   *
   * @param ids - list of entity names
   * @return a list of EntityInfos matching the ids given
   * @throws RetrievalException if any of the requested ids is not found
   * @throws IllegalArgumentException if the list of ids is empty
   */
  public List<EntityInfo> getEntities(List<String> ids) {
    if (ids.size() == 0) {
      throw new IllegalArgumentException("ids cannot be empty");
    }
    Set<String> dedupIds = Sets.newHashSet(ids);

    List<EntityInfo> entityInfos = this.entityInfoRepository.findAllById(dedupIds);
    if (entityInfos.size() < dedupIds.size()) {
      throw new RetrievalException(
          "unable to retrieve all entities requested " + ids);
    }
    return entityInfos;
  }

  /**
   * Retrieves all entities in the registry
   *
   * @return list of EntityInfos
   * @throws RetrievalException if retrieval fails
   */
  public List<EntityInfo> listEntities() {
    return this.entityInfoRepository.findAll();
  }

  /**
   * Retrieve a set of feature infos from the registry.
   *
   * @param ids - list of feature ids
   * @return a list of FeatureInfos matching the ids given
   * @throws RetrievalException if any of the requested ids is not found
   * @throws IllegalArgumentException if the list of ids is empty
   */
  public List<FeatureInfo> getFeatures(List<String> ids) {
    if (ids.size() == 0) {
      throw new IllegalArgumentException("ids cannot be empty");
    }
    Set<String> dedupIds = Sets.newHashSet(ids);

    List<FeatureInfo> featureInfos = this.featureInfoRepository.findAllById(dedupIds);
    if (featureInfos.size() < dedupIds.size()) {
      throw new RetrievalException(
          "unable to retrieve all features requested: " + ids);
    }
    return featureInfos;
  }

  /**
   * Retrieves all features in the registry
   *
   * @return list of FeatureInfos
   * @throws RetrievalException if retrieval fails
   */
  public List<FeatureInfo> listFeatures() {
    return this.featureInfoRepository.findAll();
  }

  /**
   * Retrieve a set of feature group infos from the registry.
   *
   * @param ids - list of feature group ids
   * @return a list of FeatureGroupInfos matching the ids given
   * @throws RetrievalException if any of the requested ids is not found
   * @throws IllegalArgumentException if the list of ids is empty
   */
  public List<FeatureGroupInfo> getFeatureGroups(List<String> ids) {
    if (ids.size() == 0) {
      throw new IllegalArgumentException("ids cannot be empty");
    }
    Set<String> dedupIds = Sets.newHashSet(ids);

    List<FeatureGroupInfo> featureGroupInfos = this.featureGroupInfoRepository
        .findAllById(dedupIds);
    if (featureGroupInfos.size() < dedupIds.size()) {
      throw new RetrievalException(
          "unable to retrieve all feature groups requested " + dedupIds);
    }
    return featureGroupInfos;
  }

  /**
   * Retrieves all feature groups in the registry
   *
   * @return list of FeatureGroupInfos
   * @throws RetrievalException if retrieval fails
   */
  public List<FeatureGroupInfo> listFeatureGroups() {
    return this.featureGroupInfoRepository.findAll();
  }

  /**
   * Retrieve a set of storage infos from the registry.
   *
   * @param ids - List of storage ids
   * @return a list of StorageInfos matching the ids given
   * @throws RetrievalException if any of the requested ids is not found
   * @throws IllegalArgumentException if the list of ids is empty
   */
  public List<StorageInfo> getStorage(List<String> ids) {
    if (ids.size() == 0) {
      throw new IllegalArgumentException("ids cannot be empty");
    }
    Set<String> dedupIds = Sets.newHashSet(ids);
    List<StorageInfo> storageInfos = Lists.newArrayList();
    StorageSpecs storageSpecs = getStorageSpecs();
    Map<String, StorageSpec> map = new HashMap<>();
    if (storageSpecs.getServingStorageSpec() != null) {
      map.put(storageSpecs.getServingStorageSpec().getId(), storageSpecs.getServingStorageSpec());
    }
    if (storageSpecs.getWarehouseStorageSpec() != null) {
      map.put(storageSpecs.getWarehouseStorageSpec().getId(),
          storageSpecs.getWarehouseStorageSpec());
    }
    for (String id : dedupIds) {
      if (map.containsKey(id)) {
        storageInfos.add(new StorageInfo(map.get(id)));
      }
    }
    if (dedupIds.size() != storageInfos.size()) {
      throw new RetrievalException(
          "unable to retrieve all storage requested: " + ids);

    }
    return storageInfos;
  }

  /**
   * Retrieves all storage specs in the registry
   *
   * @return list of StorageInfos
   * @throws RetrievalException if retrieval fails
   */
  public List<StorageInfo> listStorage() {
    return Lists.newArrayList(
        new StorageInfo(getStorageSpecs().getServingStorageSpec()),
        new StorageInfo(getStorageSpecs().getWarehouseStorageSpec()));
  }

  /**
   * Applies the given feature spec to the registry. If the feature does not yet exist, it will be
   * registered to the system. If it does, the existing feature will be updated with the new
   * information.
   *
   * <p>Note that specifications that will affect downstream resources (e.g. id, storage location)
   * cannot be changed.
   *
   * @param spec FeatureSpec
   * @return registered FeatureInfo
   * @throws RegistrationException if registration fails
   */
  public FeatureInfo applyFeature(FeatureSpec spec) {
    try {
      FeatureInfo featureInfo = featureInfoRepository.findById(spec.getId()).orElse(null);
      Action action;
      if (featureInfo != null) {
        featureInfo.update(spec);
        action = Action.UPDATE;
      } else {
        EntityInfo entity = entityInfoRepository.findById(spec.getEntity()).orElse(null);
        FeatureGroupInfo featureGroupInfo =
            featureGroupInfoRepository.findById(spec.getGroup()).orElse(null);
        featureInfo = new FeatureInfo(spec, entity, featureGroupInfo);
        FeatureInfo resolvedFeatureInfo = featureInfo.resolve();
        FeatureSpec resolvedFeatureSpec = resolvedFeatureInfo.getFeatureSpec();
        schemaManager.registerFeature(resolvedFeatureSpec);
        action = Action.REGISTER;
      }
      FeatureInfo out = featureInfoRepository.saveAndFlush(featureInfo);
      if (!out.getId().equals(spec.getId())) {
        throw new RegistrationException("failed to register or update feature");
      }
      AuditLogger.log(
          Resource.FEATURE,
          spec.getId(),
          action,
          "Feature applied: %s",
          JsonFormat.printer().print(spec));
      return out;

    } catch (Exception e) {
      throw new RegistrationException(
          Strings.lenientFormat("Failed to apply feature %s: %s", spec, e.getMessage()), e);
    }
  }

  /**
   * Applies the given feature group spec to the registry. If the entity does not yet exist, it will
   * be registered to the system. Otherwise, the fields will be updated as per the given feature
   * group spec.
   *
   * @param spec FeatureGroupSpec
   * @return registered FeatureGroupInfo
   * @throws RegistrationException if registration fails
   */
  public FeatureGroupInfo applyFeatureGroup(FeatureGroupSpec spec) {
    try {
      FeatureGroupInfo featureGroupInfo =
          featureGroupInfoRepository.findById(spec.getId()).orElse(null);
      Action action;
      if (featureGroupInfo != null) {
        featureGroupInfo.update(spec);
        action = Action.UPDATE;
      } else {
        featureGroupInfo = new FeatureGroupInfo(spec);
        action = Action.REGISTER;
      }
      FeatureGroupInfo out = featureGroupInfoRepository.saveAndFlush(featureGroupInfo);
      if (!out.getId().equals(spec.getId())) {
        throw new RegistrationException("failed to register or update feature group");
      }
      AuditLogger.log(
          Resource.FEATURE_GROUP,
          spec.getId(),
          action,
          "Feature group applied: %s",
          JsonFormat.printer().print(spec));
      return out;
    } catch (Exception e) {
      throw new RegistrationException(
          Strings.lenientFormat(
              "Failed to register new feature group %s: %s", spec, e.getMessage()),
          e);
    }
  }

  /**
   * Applies the given entity spec to the registry. If the entity does not yet exist, it will be
   * registered to the system. Otherwise, the fields will be updated as per the given entity spec.
   *
   * @param spec EntitySpec
   * @return registered EntityInfo
   * @throws RegistrationException if registration fails
   */
  public EntityInfo applyEntity(EntitySpec spec) {
    EntityInfo out;
    try {
      EntityInfo entityInfo = entityInfoRepository.findById(spec.getName()).orElse(null);
      Action action;
      if (entityInfo != null) {
        entityInfo.update(spec);
        action = Action.UPDATE;
        out = entityInfoRepository.saveAndFlush(entityInfo);
        if (!out.getName().equals(spec.getName())) {
          throw new RegistrationException("failed to register or update entity");
        }
      } else {
        entityInfo = new EntityInfo(spec);
        FeatureStreamTopic topic = featureStreamService.provisionTopic(entityInfo);
        entityInfo.setTopic(topic);
        action = Action.REGISTER;
        out = entityInfoRepository.saveAndFlush(entityInfo);
        if (!out.getName().equals(spec.getName())) {
          featureStreamService.deleteTopic(topic);
          throw new RegistrationException("failed to register or update entity");
        }
      }
      AuditLogger.log(
          Resource.ENTITY, spec.getName(), action, "Entity: %s",
          JsonFormat.printer().print(spec));
      return out;
    } catch (Exception e) {
      throw new RegistrationException(
          Strings.lenientFormat("Failed to apply entity %s: %s", spec, e.getMessage()), e);
    }
  }
}
