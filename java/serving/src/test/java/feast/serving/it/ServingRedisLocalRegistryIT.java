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

import feast.proto.core.RegistryProto;
import feast.serving.service.config.ApplicationProperties;

public class ServingRedisLocalRegistryIT extends ServingBaseTests {
  @Override
  ApplicationProperties.FeastProperties createFeastProperties() {
    return TestUtils.createBasicFeastProperties(
        environment.getServiceHost("redis", 6379), environment.getServicePort("redis", 6379));
  }

  @Override
  void updateRegistryFile(RegistryProto.Registry registry) {}

  @Override
  public void shouldRefreshRegistryAndServeNewFeatures() throws InterruptedException {}
}
