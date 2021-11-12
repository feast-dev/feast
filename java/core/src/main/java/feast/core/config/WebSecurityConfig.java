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
package feast.core.config;

import java.util.ArrayList;
import java.util.List;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.config.annotation.web.configuration.WebSecurityConfigurerAdapter;

/**
 * WebSecurityConfig disables auto configuration of Spring HTTP Security and allows security methods
 * to be overridden
 */
@Configuration
@EnableWebSecurity
public class WebSecurityConfig extends WebSecurityConfigurerAdapter {

  private final FeastProperties feastProperties;

  @Autowired
  public WebSecurityConfig(FeastProperties feastProperties) {
    this.feastProperties = feastProperties;
  }

  /**
   * Allows for custom web security rules to be applied.
   *
   * @param http {@link HttpSecurity} for configuring web based security
   * @throws Exception unexpected exception
   */
  @Override
  protected void configure(HttpSecurity http) throws Exception {
    List<String> matchersToBypass = new ArrayList<>(List.of("/actuator/**", "/metrics/**"));

    if (feastProperties.securityProperties().isDisableRestControllerAuth()) {
      matchersToBypass.add("/api/v1/**");
      matchersToBypass.add("/api/v2/**");
    }

    // Bypasses security/authentication for the following paths
    http.authorizeRequests()
        .antMatchers(matchersToBypass.toArray(new String[0]))
        .permitAll()
        .anyRequest()
        .authenticated()
        .and()
        .csrf()
        .disable();
  }
}
