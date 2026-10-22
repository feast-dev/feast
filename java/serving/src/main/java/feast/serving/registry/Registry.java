/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright 2018-2021 The Feast Authors
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
package feast.serving.registry;

import feast.proto.core.*;
import feast.proto.core.FeatureServiceProto.FeatureService;
import feast.proto.core.FeatureViewProto.FeatureView;
import feast.proto.core.OnDemandFeatureViewProto.OnDemandFeatureView;
import feast.proto.serving.ServingAPIProto;
import feast.serving.exception.SpecRetrievalException;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public class Registry {
  private final RegistryProto.Registry registry;
  private final Map<String, FeatureViewProto.FeatureViewSpec> featureViewNameToSpec;
  private Map<String, OnDemandFeatureViewProto.OnDemandFeatureViewSpec>
      onDemandFeatureViewNameToSpec;
  private final Map<String, FeatureServiceProto.FeatureServiceSpec> featureServiceNameToSpec;
  private final Map<String, String> entityNameToJoinKey;

  Registry(RegistryProto.Registry registry) {
    this.registry = registry;
    List<FeatureViewProto.FeatureViewSpec> featureViewSpecs =
        registry.getFeatureViewsList().stream()
            .map(FeatureView::getSpec)
            .collect(Collectors.toList());
    this.featureViewNameToSpec =
        featureViewSpecs.stream()
            .collect(
                Collectors.toMap(FeatureViewProto.FeatureViewSpec::getName, Function.identity()));
    List<OnDemandFeatureViewProto.OnDemandFeatureViewSpec> onDemandFeatureViewSpecs =
        registry.getOnDemandFeatureViewsList().stream()
            .map(OnDemandFeatureView::getSpec)
            .collect(Collectors.toList());
    this.onDemandFeatureViewNameToSpec =
        onDemandFeatureViewSpecs.stream()
            .collect(
                Collectors.toMap(
                    OnDemandFeatureViewProto.OnDemandFeatureViewSpec::getName,
                    Function.identity()));
    this.featureServiceNameToSpec =
        registry.getFeatureServicesList().stream()
            .map(FeatureService::getSpec)
            .collect(
                Collectors.toMap(
                    FeatureServiceProto.FeatureServiceSpec::getName, Function.identity()));
    this.entityNameToJoinKey =
        registry.getEntitiesList().stream()
            .map(EntityProto.Entity::getSpec)
            .collect(
                Collectors.toMap(
                    EntityProto.EntitySpecV2::getName, EntityProto.EntitySpecV2::getJoinKey));
  }

  public RegistryProto.Registry getRegistry() {
    return this.registry;
  }

  public FeatureViewProto.FeatureViewSpec getFeatureViewSpec(
      ServingAPIProto.FeatureReferenceV2 featureReference) {
    String featureViewName = featureReference.getFeatureViewName();
    if (featureViewNameToSpec.containsKey(featureViewName)) {
      return featureViewNameToSpec.get(featureViewName);
    }
    throw new SpecRetrievalException(
        String.format("Unable to find feature view with name: %s", featureViewName));
  }

  public FeatureProto.FeatureSpecV2 getFeatureSpec(
      ServingAPIProto.FeatureReferenceV2 featureReference) {
    final FeatureViewProto.FeatureViewSpec spec = this.getFeatureViewSpec(featureReference);
    for (final FeatureProto.FeatureSpecV2 featureSpec : spec.getFeaturesList()) {
      if (featureSpec.getName().equals(featureReference.getFeatureName())) {
        return featureSpec;
      }
    }

    throw new SpecRetrievalException(
        String.format(
            "Unable to find feature with name: %s in feature view: %s",
            featureReference.getFeatureName(), featureReference.getFeatureViewName()));
  }

  public OnDemandFeatureViewProto.OnDemandFeatureViewSpec getOnDemandFeatureViewSpec(
      ServingAPIProto.FeatureReferenceV2 featureReference) {
    String onDemandFeatureViewName = featureReference.getFeatureViewName();
    if (onDemandFeatureViewNameToSpec.containsKey(onDemandFeatureViewName)) {
      return onDemandFeatureViewNameToSpec.get(onDemandFeatureViewName);
    }
    throw new SpecRetrievalException(
        String.format(
            "Unable to find on demand feature view with name: %s", onDemandFeatureViewName));
  }

  public boolean isOnDemandFeatureReference(ServingAPIProto.FeatureReferenceV2 featureReference) {
    String onDemandFeatureViewName = featureReference.getFeatureViewName();
    return onDemandFeatureViewNameToSpec.containsKey(onDemandFeatureViewName);
  }

  public FeatureServiceProto.FeatureServiceSpec getFeatureServiceSpec(String name) {
    FeatureServiceProto.FeatureServiceSpec spec = featureServiceNameToSpec.get(name);
    if (spec == null) {
      throw new SpecRetrievalException(
          String.format("Unable to find feature service with name: %s", name));
    }
    return spec;
  }

  public String getEntityJoinKey(String name) {
    String joinKey = entityNameToJoinKey.get(name);
    if (joinKey == null) {
      throw new SpecRetrievalException(String.format("Unable to find entity with name: %s", name));
    }
    return joinKey;
  }
}
