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

import feast.proto.core.FeatureProto;
import feast.proto.core.FeatureViewProto;
import feast.proto.core.OnDemandFeatureViewProto;
import feast.proto.core.RegistryProto;
import feast.proto.serving.ServingAPIProto;
import feast.serving.exception.SpecRetrievalException;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public class Registry {
  private final RegistryProto.Registry registry;
  private Map<String, FeatureViewProto.FeatureViewSpec> featureViewNameToSpec;
  private Map<String, OnDemandFeatureViewProto.OnDemandFeatureViewSpec>
      onDemandFeatureViewNameToSpec;

  Registry(RegistryProto.Registry registry) {
    this.registry = registry;
    List<FeatureViewProto.FeatureViewSpec> featureViewSpecs =
        registry.getFeatureViewsList().stream()
            .map(fv -> fv.getSpec())
            .collect(Collectors.toList());
    this.featureViewNameToSpec =
        featureViewSpecs.stream()
            .collect(
                Collectors.toMap(FeatureViewProto.FeatureViewSpec::getName, Function.identity()));
    List<OnDemandFeatureViewProto.OnDemandFeatureViewSpec> onDemandFeatureViewSpecs =
        registry.getOnDemandFeatureViewsList().stream()
            .map(odfv -> odfv.getSpec())
            .collect(Collectors.toList());
    this.onDemandFeatureViewNameToSpec =
        onDemandFeatureViewSpecs.stream()
            .collect(
                Collectors.toMap(
                    OnDemandFeatureViewProto.OnDemandFeatureViewSpec::getName,
                    Function.identity()));
  }

  public RegistryProto.Registry getRegistry() {
    return this.registry;
  }

  public FeatureViewProto.FeatureViewSpec getFeatureViewSpec(
      String projectName, ServingAPIProto.FeatureReferenceV2 featureReference) {
    String featureViewName = featureReference.getFeatureTable();
    if (featureViewNameToSpec.containsKey(featureViewName)) {
      return featureViewNameToSpec.get(featureViewName);
    }
    throw new SpecRetrievalException(
        String.format("Unable to find feature view with name: %s", featureViewName));
  }

  public FeatureProto.FeatureSpecV2 getFeatureSpec(
      String projectName, ServingAPIProto.FeatureReferenceV2 featureReference) {
    final FeatureViewProto.FeatureViewSpec spec =
        this.getFeatureViewSpec(projectName, featureReference);
    for (final FeatureProto.FeatureSpecV2 featureSpec : spec.getFeaturesList()) {
      if (featureSpec.getName().equals(featureReference.getName())) {
        return featureSpec;
      }
    }

    throw new SpecRetrievalException(
        String.format(
            "Unable to find feature with name: %s in feature view: %s",
            featureReference.getName(), featureReference.getFeatureTable()));
  }

  public OnDemandFeatureViewProto.OnDemandFeatureViewSpec getOnDemandFeatureViewSpec(
      String projectName, ServingAPIProto.FeatureReferenceV2 featureReference) {
    String onDemandFeatureViewName = featureReference.getFeatureTable();
    if (onDemandFeatureViewNameToSpec.containsKey(onDemandFeatureViewName)) {
      return onDemandFeatureViewNameToSpec.get(onDemandFeatureViewName);
    }
    throw new SpecRetrievalException(
        String.format(
            "Unable to find on demand feature view with name: %s", onDemandFeatureViewName));
  }

  public boolean isOnDemandFeatureReference(ServingAPIProto.FeatureReferenceV2 featureReference) {
    String onDemandFeatureViewName = featureReference.getFeatureTable();
    return onDemandFeatureViewNameToSpec.containsKey(onDemandFeatureViewName);
  }
}
