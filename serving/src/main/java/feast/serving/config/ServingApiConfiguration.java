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

package feast.serving.config;

import com.google.common.base.Strings;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import feast.serving.service.CachedSpecStorage;
import feast.serving.service.CoreService;
import feast.serving.service.FeatureStorageRegistry;
import feast.serving.service.SpecStorage;
import feast.specs.StorageSpecProto.StorageSpec;
import io.opentracing.Tracer;
import io.opentracing.contrib.concurrent.TracedExecutorService;
import java.lang.reflect.Type;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.converter.HttpMessageConverter;
import org.springframework.http.converter.protobuf.ProtobufJsonFormatHttpMessageConverter;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

/**
 * Global bean configuration.
 */
@Slf4j
@Configuration
public class ServingApiConfiguration implements WebMvcConfigurer {

  @Autowired
  private ProtobufJsonFormatHttpMessageConverter protobufConverter;
  private ScheduledExecutorService scheduledExecutorService =
      Executors.newSingleThreadScheduledExecutor();

  private static Map<String, String> convertJsonStringToMap(String jsonString) {
    if (jsonString == null || jsonString.equals("") || jsonString.equals("{}")) {
      return Collections.emptyMap();
    }
    Type stringMapType = new TypeToken<Map<String, String>>() {
    }.getType();
    return new Gson().fromJson(jsonString, stringMapType);
  }


  @Bean
  public AppConfig getAppConfig(
      @Value("${feast.redispool.maxsize}") int redisPoolMaxSize,
      @Value("${feast.redispool.maxidle}") int redisPoolMaxIdle,
      @Value("${feast.maxentity}") int maxEntityPerBatch,
      @Value("${feast.timeout}") int timeout) {
    return AppConfig.builder()
        .maxEntityPerBatch(maxEntityPerBatch)
        .redisMaxPoolSize(redisPoolMaxSize)
        .redisMaxIdleSize(redisPoolMaxIdle)
        .timeout(timeout)
        .build();
  }

  @Bean
  public SpecStorage getCoreServiceSpecStorage(
      @Value("${feast.core.host}") String coreServiceHost,
      @Value("${feast.core.grpc.port}") String coreServicePort,
      @Value("${feast.cacheDurationMinute}") int cacheDurationMinute) {
    final CachedSpecStorage cachedSpecStorage =
        new CachedSpecStorage(new CoreService(coreServiceHost, Integer.parseInt(coreServicePort)));

    // reload all specs including new ones periodically
    scheduledExecutorService.schedule(
        cachedSpecStorage::populateCache, cacheDurationMinute, TimeUnit.MINUTES);

    // load all specs during start up
    try {
      cachedSpecStorage.populateCache();
    } catch (Exception e) {
      log.error("Unable to preload feast's spec");
    }
    return cachedSpecStorage;
  }

  @Bean
  public FeatureStorageRegistry getFeatureStorageRegistry(
      @Value("${feast.store.serving.type}") String storageType,
      @Value("${feast.store.serving.options}") String storageOptions,
      AppConfig appConfig, Tracer tracer) {
    storageOptions = Strings.isNullOrEmpty(storageOptions) ? "{}" : storageOptions;
    Map<String, String> optionsMap = convertJsonStringToMap(storageOptions);
    StorageSpec storageSpec = StorageSpec.getDefaultInstance();
    if (Strings.isNullOrEmpty(storageType)) {
      storageSpec = StorageSpec.newBuilder()
          .setId("SERVING")
          .setType(storageType)
          .putAllOptions(optionsMap)
          .build();
    }
    FeatureStorageRegistry registry = new FeatureStorageRegistry(appConfig, tracer);
    try {
      registry.connect(storageSpec);
    } catch (Exception e) {
      log.error(
          "Unable to create a pre-populated storage registry, connection will be made in ad-hoc basis",
          e);
    }
    return registry;
  }

  @Bean
  public ListeningExecutorService getExecutorService(
      Tracer tracer, @Value("${feast.threadpool.max}") int maxPoolSize) {

    ExecutorService executor = Executors.newFixedThreadPool(maxPoolSize);
    return MoreExecutors.listeningDecorator(new TracedExecutorService(executor, tracer));
  }

  @Bean
  ProtobufJsonFormatHttpMessageConverter protobufHttpMessageConverter() {
    return new ProtobufJsonFormatHttpMessageConverter();
  }

  @Override
  public void configureMessageConverters(List<HttpMessageConverter<?>> converters) {
    converters.add(protobufConverter);
  }
}
