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
package feast.common.util;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import feast.common.logging.AuditLogger;
import feast.common.logging.config.LoggingProperties;
import org.springframework.boot.info.BuildProperties;

public class TestUtil {
  /** Setup the audit logger. This call is required to use the audit logger when testing. */
  public static void setupAuditLogger() {
    LoggingProperties.AuditLogProperties properties = new LoggingProperties.AuditLogProperties();
    properties.setEnabled(true);
    LoggingProperties loggingProperties = new LoggingProperties();
    loggingProperties.setAudit(properties);

    BuildProperties buildProperties = mock(BuildProperties.class);
    when(buildProperties.getArtifact()).thenReturn("feast-core");
    when(buildProperties.getVersion()).thenReturn("0.6");

    new AuditLogger(loggingProperties, buildProperties);
  }
}
