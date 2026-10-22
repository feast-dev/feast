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

import com.google.protobuf.Duration;
import feast.proto.core.FeatureProto;
import feast.proto.core.FeatureServiceProto;
import feast.proto.core.FeatureViewProto;
import feast.proto.core.OnDemandFeatureViewProto;
import feast.proto.core.RegistryProto;
import feast.proto.serving.ServingAPIProto;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/*
 *
 * Read-only access to Feast registry.
 * Registry is obtained by calling specific storage implementation: eg, Local, GCS, AWS S3.
 * All data is being then cached.
 * It is possible to refresh registry (reload from storage) on configured interval.
 *
 * */
public class RegistryRepository {
  private Registry registry;
  private RegistryFile registryFile;

  public RegistryRepository(RegistryFile registryFile, int refreshIntervalSecs) {
    this.registryFile = registryFile;
    this.registry = new Registry(this.registryFile.getContent());

    if (refreshIntervalSecs > 0) {
      setupPeriodicalRefresh(refreshIntervalSecs);
    }
  }

  public RegistryRepository(Registry registry) {
    this.registry = registry;
  }

  private void setupPeriodicalRefresh(int seconds) {
    Executors.newSingleThreadScheduledExecutor(
            r -> {
              Thread t = Executors.defaultThreadFactory().newThread(r);
              t.setDaemon(true);
              return t;
            })
        .scheduleWithFixedDelay(this::refresh, seconds, seconds, TimeUnit.SECONDS);
  }

  private void refresh() {
    Optional<RegistryProto.Registry> registryProto = this.registryFile.getContentIfModified();
    if (registryProto.isEmpty()) {
      return;
    }

    this.registry = new Registry(registryProto.get());
  }

  public FeatureViewProto.FeatureViewSpec getFeatureViewSpec(
      ServingAPIProto.FeatureReferenceV2 featureReference) {
    return this.registry.getFeatureViewSpec(featureReference);
  }

  public FeatureProto.FeatureSpecV2 getFeatureSpec(
      ServingAPIProto.FeatureReferenceV2 featureReference) {
    return this.registry.getFeatureSpec(featureReference);
  }

  public OnDemandFeatureViewProto.OnDemandFeatureViewSpec getOnDemandFeatureViewSpec(
      ServingAPIProto.FeatureReferenceV2 featureReference) {
    return this.registry.getOnDemandFeatureViewSpec(featureReference);
  }

  public boolean isOnDemandFeatureReference(ServingAPIProto.FeatureReferenceV2 featureReference) {
    return this.registry.isOnDemandFeatureReference(featureReference);
  }

  public FeatureServiceProto.FeatureServiceSpec getFeatureServiceSpec(String name) {
    return this.registry.getFeatureServiceSpec(name);
  }

  public Duration getMaxAge(ServingAPIProto.FeatureReferenceV2 featureReference) {
    return getFeatureViewSpec(featureReference).getTtl();
  }

  public List<String> getEntitiesList(ServingAPIProto.FeatureReferenceV2 featureReference) {
    return getFeatureViewSpec(featureReference).getEntitiesList();
  }

  public String getEntityJoinKey(String name) {
    return this.registry.getEntityJoinKey(name);
  }
}
