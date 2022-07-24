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
package feast.common.logging.config;

import feast.common.validators.OneOfStrings;
import javax.validation.constraints.NotNull;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class LoggingProperties {
  @NotNull private AuditLogProperties audit;

  @Getter
  @Setter
  public static class AuditLogProperties {
    // Whether to enable/disable audit logging entirely.
    private boolean enabled;

    private MessageLogging messageLogging;

    @Getter
    @Setter
    public static class MessageLogging {
      // Whether to enable/disable message level (ie request/response) audit logging.
      private boolean enabled;

      // Whether to log to console or fluentd
      @OneOfStrings({"console", "fluentd"})
      private String destination;

      // fluentD service host for external (request/response) logging.
      private String fluentdHost;

      // fluentD service port for external (request/response) logging.
      private Integer fluentdPort;
    }
  }
}
