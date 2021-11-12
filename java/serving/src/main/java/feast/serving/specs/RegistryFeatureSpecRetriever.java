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
package feast.serving.specs;

import com.google.protobuf.Duration;
import feast.proto.core.FeatureProto;
import feast.proto.core.FeatureViewProto;
import feast.proto.core.OnDemandFeatureViewProto;
import feast.proto.core.RegistryProto;
import feast.proto.serving.ServingAPIProto;
import feast.serving.exception.SpecRetrievalException;
import feast.serving.registry.RegistryRepository;
import java.util.List;

public class RegistryFeatureSpecRetriever implements FeatureSpecRetriever {
  private final RegistryRepository registryRepository;

  public RegistryFeatureSpecRetriever(RegistryRepository registryRepository) {
    this.registryRepository = registryRepository;
  }

  @Override
  public Duration getMaxAge(
      String projectName, ServingAPIProto.FeatureReferenceV2 featureReference) {
    final RegistryProto.Registry registry = this.registryRepository.getRegistry();
    for (final FeatureViewProto.FeatureView featureView : registry.getFeatureViewsList()) {
      if (featureView.getSpec().getName().equals(featureReference.getFeatureTable())) {
        return featureView.getSpec().getTtl();
      }
    }
    throw new SpecRetrievalException(
        String.format(
            "Unable to find feature view with name: %s", featureReference.getFeatureTable()));
  }

  @Override
  public List<String> getEntitiesList(
      String projectName, ServingAPIProto.FeatureReferenceV2 featureReference) {
    final RegistryProto.Registry registry = this.registryRepository.getRegistry();
    for (final FeatureViewProto.FeatureView featureView : registry.getFeatureViewsList()) {
      if (featureView.getSpec().getName().equals(featureReference.getFeatureTable())) {
        return featureView.getSpec().getEntitiesList();
      }
    }
    throw new SpecRetrievalException(
        String.format(
            "Unable to find feature view with name: %s", featureReference.getFeatureTable()));
  }

  @Override
  public FeatureProto.FeatureSpecV2 getFeatureSpec(
      String projectName, ServingAPIProto.FeatureReferenceV2 featureReference) {
    return this.registryRepository.getFeatureSpec(projectName, featureReference);
  }

  @Override
  public FeatureViewProto.FeatureViewSpec getBatchFeatureViewSpec(
      String projectName, ServingAPIProto.FeatureReferenceV2 featureReference) {
    return this.registryRepository.getFeatureViewSpec(projectName, featureReference);
  }

  @Override
  public OnDemandFeatureViewProto.OnDemandFeatureViewSpec getOnDemandFeatureViewSpec(
      String projectName, ServingAPIProto.FeatureReferenceV2 featureReference) {
    return this.registryRepository.getOnDemandFeatureViewSpec(projectName, featureReference);
  }

  @Override
  public boolean isOnDemandFeatureReference(ServingAPIProto.FeatureReferenceV2 featureReference) {
    return this.registryRepository.isOnDemandFeatureReference(featureReference);
  }
}
