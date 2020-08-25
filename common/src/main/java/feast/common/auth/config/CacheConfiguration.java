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
package feast.common.auth.config;

import com.google.common.cache.CacheBuilder;
import feast.common.auth.utils.AuthUtils;
import java.lang.reflect.Method;
import java.util.concurrent.TimeUnit;
import lombok.Getter;
import lombok.Setter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cache.Cache;
import org.springframework.cache.CacheManager;
import org.springframework.cache.annotation.CachingConfigurer;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.cache.concurrent.ConcurrentMapCache;
import org.springframework.cache.concurrent.ConcurrentMapCacheManager;
import org.springframework.cache.interceptor.CacheErrorHandler;
import org.springframework.cache.interceptor.CacheResolver;
import org.springframework.cache.interceptor.KeyGenerator;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.core.Authentication;

/** CacheConfiguration class defines Cache settings for HttpAuthorizationProvider class. */
@Configuration
@EnableCaching
@Setter
@Getter
public class CacheConfiguration implements CachingConfigurer {

  private static final int CACHE_SIZE = 10000;

  public static int TTL = 60;

  public static final String AUTHORIZATION_CACHE = "authorization";

  @Autowired SecurityProperties securityProperties;

  @Bean
  public CacheManager cacheManager() {
    ConcurrentMapCacheManager cacheManager =
        new ConcurrentMapCacheManager(AUTHORIZATION_CACHE) {

          @Override
          protected Cache createConcurrentMapCache(final String name) {
            return new ConcurrentMapCache(
                name,
                CacheBuilder.newBuilder()
                    .expireAfterWrite(TTL, TimeUnit.SECONDS)
                    .maximumSize(CACHE_SIZE)
                    .build()
                    .asMap(),
                false);
          }
        };

    return cacheManager;
  }

  /*
   * KeyGenerator used by {@link Cacheable} for caching authorization requests.
   * Key format : checkAccessToProject-<projectId>-<subjectClaim>
   */
  @Bean
  public KeyGenerator authKeyGenerator() {
    return (Object target, Method method, Object... params) -> {
      String projectId = (String) params[0];
      Authentication authentication = (Authentication) params[1];
      String subject =
          AuthUtils.getSubjectFromAuth(
              authentication,
              securityProperties.getAuthorization().getOptions().get("subjectClaim"));
      return String.format("%s-%s-%s", method.getName(), projectId, subject);
    };
  }

  @Override
  public CacheResolver cacheResolver() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public KeyGenerator keyGenerator() {
    return null;
  }

  @Override
  public CacheErrorHandler errorHandler() {
    // TODO Auto-generated method stub
    return null;
  }
}
