/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright 2018-2020 The Feast Authors
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
package feast.serving.it;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import feast.proto.core.RegistryProto;
import feast.serving.config.ApplicationProperties;

public class ServingRedisLocalRegistryIT extends ServingBase {
  @Override
  ApplicationProperties.FeastProperties createFeastProperties() {
    final ApplicationProperties.FeastProperties feastProperties =
        new ApplicationProperties.FeastProperties();
    feastProperties.setRegistry("src/test/resources/docker-compose/feast10/registry.db");
    feastProperties.setRegistryRefreshInterval(1);

    feastProperties.setActiveStore("online");

    feastProperties.setStores(
        ImmutableList.of(
            new ApplicationProperties.Store(
                "online", "REDIS", ImmutableMap.of("host", "localhost", "port", "6379"))));

    return feastProperties;
  }

  @Override
  void updateRegistryFile(RegistryProto.Registry registry) {}

  @Override
  public void shouldRefreshRegistryAndServeNewFeatures() throws InterruptedException {}
}
