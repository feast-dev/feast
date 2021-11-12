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
package feast.common.it;

import com.google.auto.value.AutoValue;
import java.util.HashMap;
import java.util.Map;
import org.springframework.boot.Banner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.core.env.MapPropertySource;
import org.springframework.core.env.StandardEnvironment;
import org.springframework.util.SocketUtils;
import org.testcontainers.containers.PostgreSQLContainer;

@AutoValue
public abstract class ExternalApp {
  private ConfigurableApplicationContext appContext;

  abstract PostgreSQLContainer<?> getPostgreSQL();

  abstract String getName();

  abstract int getGRPCPort();

  abstract int getWebPort();

  abstract Map<String, Object> getProperties();

  abstract Class<?> getSpringApplication();

  public static Builder builder() {
    return new AutoValue_ExternalApp.Builder()
        .setProperties(new HashMap<>())
        .setWebPort(SocketUtils.findAvailableTcpPort());
  }

  @AutoValue.Builder
  public interface Builder {
    Builder setSpringApplication(Class<?> app);

    Builder setName(String name);

    Builder setPostgreSQL(PostgreSQLContainer<?> psql);

    Builder setGRPCPort(int port);

    Builder setWebPort(int port);

    Builder setProperties(Map<String, Object> properties);

    ExternalApp build();
  }

  public void start() {
    HashMap<String, Object> properties = new HashMap<>(getProperties());
    properties.put("spring.datasource.url", getPostgreSQL().getJdbcUrl());
    properties.put("spring.datasource.username", getPostgreSQL().getUsername());
    properties.put("spring.datasource.password", getPostgreSQL().getPassword());
    properties.put("grpc.server.port", getGRPCPort());
    properties.put("server.port", getWebPort());

    StandardEnvironment env = new StandardEnvironment();
    env.setDefaultProfiles(getName());
    env.getPropertySources().addFirst(new MapPropertySource("primary", properties));

    appContext =
        new SpringApplicationBuilder(getSpringApplication())
            .environment(env)
            .bannerMode(Banner.Mode.OFF)
            .run();
  }

  public void stop() {
    SpringApplication.exit(appContext);
  }
}
