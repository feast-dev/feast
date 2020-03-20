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
package feast.core.config;

import java.util.Map;
import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

@Getter
@Setter
@ConfigurationProperties(prefix = "feast", ignoreInvalidFields = true)
public class FeastProperties {

  private String version;
  private JobProperties jobs;
  private StreamProperties stream;
  private SecurityProperties security;

  @Getter
  @Setter
  public static class JobProperties {

    private String runner;
    private Map<String, String> options;
    private MetricsProperties metrics;
    private JobUpdatesProperties updates;
  }

  @Getter
  @Setter
  public static class JobUpdatesProperties {

    private long timeoutSeconds;
  }

  @Getter
  @Setter
  public static class StreamProperties {

    private String type;
    private Map<String, String> options;
  }

  @Getter
  @Setter
  public static class MetricsProperties {

    private boolean enabled;
    private String type;
    private String host;
    private int port;
  }

  @Getter
  @Setter
  public static class SecurityProperties {

    private AuthenticationProperties authentication;
    private AuthorizationProperties authorization;

    @Getter
    @Setter
    public static class AuthenticationProperties {

      // Enable authentication
      private boolean enabled;

      // Named authentication provider to use
      private String provider;

      // K/V options to initialize the provider with
      private Map<String, String> options;
    }

    @Getter
    @Setter
    public static class AuthorizationProperties {

      // Enable authorization. Authentication must be enabled if authorization is enabled.
      private boolean enabled;

      // Named authorization provider to use.
      private String provider;

      // K/V options to initialize the provider with
      private Map<String, String> options;
    }
  }
}
