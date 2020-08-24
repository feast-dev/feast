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
package feast.jobcontroller.config;

import org.springframework.context.annotation.Configuration;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.WebSecurityConfigurerAdapter;

/**
 * WebSecurityConfig disables auto configuration of Spring HTTP Security and allows security methods
 * to be overridden
 */
@Configuration
public class WebSecurityConfig extends WebSecurityConfigurerAdapter {

  /**
   * Allows for custom web security rules to be applied.
   *
   * @param http {@link HttpSecurity} for configuring web based security
   * @throws Exception
   */
  @Override
  protected void configure(HttpSecurity http) throws Exception {
    // Bypasses security/authentication for the following paths
    http.authorizeRequests()
        // TODO: Currently allows access to all endpoints as Security has not been implemented for
        // JobController yet.
        // When security is enabled, should only allow unauthenticated access to actuator and
        // metrics endpoints.
        .antMatchers("/")
        // .antMatchers("/actuator/**", "/metrics/**")
        .permitAll()
        .anyRequest()
        .authenticated()
        .and()
        .csrf()
        .disable();
  }
}
