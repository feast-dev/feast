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
package feast.serving.config;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.protobuf.InvalidProtocolBufferException;
import feast.proto.core.StoreProto;
import feast.serving.specs.CachedSpecService;
import feast.serving.specs.CoreSpecService;
import io.grpc.CallCredentials;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class SpecServiceConfig {

  private static final Logger log = org.slf4j.LoggerFactory.getLogger(SpecServiceConfig.class);
  private String feastCoreHost;
  private int feastCorePort;
  private int feastCachedSpecServiceRefreshInterval;

  @Autowired
  public SpecServiceConfig(FeastProperties feastProperties) {
    this.feastCoreHost = feastProperties.getCoreHost();
    this.feastCorePort = feastProperties.getCoreGrpcPort();
    this.feastCachedSpecServiceRefreshInterval = feastProperties.getCoreCacheRefreshInterval();
  }

  @Bean
  public ScheduledExecutorService cachedSpecServiceScheduledExecutorService(
      CachedSpecService cachedSpecStorage) {
    ScheduledExecutorService scheduledExecutorService =
        Executors.newSingleThreadScheduledExecutor();
    // reload all specs including new ones periodically
    scheduledExecutorService.scheduleAtFixedRate(
        cachedSpecStorage::scheduledPopulateCache,
        feastCachedSpecServiceRefreshInterval,
        feastCachedSpecServiceRefreshInterval,
        TimeUnit.SECONDS);
    return scheduledExecutorService;
  }

  @Bean
  public CachedSpecService specService(
      FeastProperties feastProperties, ObjectProvider<CallCredentials> callCredentials)
      throws InvalidProtocolBufferException, JsonProcessingException {
    CoreSpecService coreService =
        new CoreSpecService(feastCoreHost, feastCorePort, callCredentials);
    StoreProto.Store storeProto = feastProperties.getActiveStore().toProto();
    CachedSpecService cachedSpecStorage = new CachedSpecService(coreService, storeProto);
    try {
      cachedSpecStorage.populateCache();
    } catch (Exception e) {
      log.error("Unable to preload feast's spec");
    }
    return cachedSpecStorage;
  }
}
