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
package feast.common.logging;

import feast.common.logging.config.LoggingProperties;
import feast.common.logging.config.LoggingProperties.AuditLogProperties;
import feast.common.logging.entry.ActionAuditLogEntry;
import feast.common.logging.entry.AuditLogEntry;
import feast.common.logging.entry.AuditLogEntryKind;
import feast.common.logging.entry.LogResource;
import feast.common.logging.entry.LogResource.ResourceType;
import feast.common.logging.entry.MessageAuditLogEntry;
import feast.common.logging.entry.TransitionAuditLogEntry;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Marker;
import org.slf4j.MarkerFactory;
import org.slf4j.event.Level;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.info.BuildProperties;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class AuditLogger {
  private static final Marker AUDIT_MARKER = MarkerFactory.getMarker("AUDIT_MARK");
  private static AuditLogProperties properties;
  private static BuildProperties buildProperties;

  @Autowired
  public AuditLogger(LoggingProperties loggingProperties, BuildProperties buildProperties) {
    // Spring runs this constructor when creating the AuditLogger bean,
    // which allows us to populate the AuditLogger class with dependencies.
    // This allows us to use the dependencies in the AuditLogger's static methods
    AuditLogger.properties = loggingProperties.getAudit();
    AuditLogger.buildProperties = buildProperties;
  }

  /**
   * Log the handling of a Protobuf message by a service call.
   *
   * @param entryBuilder with all fields set except instance.
   */
  public static void logMessage(Level level, MessageAuditLogEntry.Builder entryBuilder) {
    log(
        level,
        entryBuilder
            .setComponent(buildProperties.getArtifact())
            .setVersion(buildProperties.getVersion())
            .build());
  }

  /**
   * Log an action being taken on a specific resource
   *
   * @param level describing the severity of the log.
   * @param action name of the action being taken on specific resource.
   * @param resourceType the type of resource being logged.
   * @param resourceId resource specific identifier identifing the instance of the resource.
   */
  public static void logAction(
      Level level, String action, ResourceType resourceType, String resourceId) {
    log(
        level,
        ActionAuditLogEntry.of(
            buildProperties.getArtifact(),
            buildProperties.getArtifact(),
            LogResource.of(resourceType, resourceId),
            action));
  }

  /**
   * Log a transition in state/status in a specific resource.
   *
   * @param level describing the severity of the log.
   * @param status name of end status which the resource transition to.
   * @param resourceType the type of resource being logged.
   * @param resourceId resource specific identifier identifing the instance of the resource.
   */
  public static void logTransition(
      Level level, String status, ResourceType resourceType, String resourceId) {
    log(
        level,
        TransitionAuditLogEntry.of(
            buildProperties.getArtifact(),
            buildProperties.getArtifact(),
            LogResource.of(resourceType, resourceId),
            status));
  }

  /**
   * Log given {@link AuditLogEntry} at the given logging {@link Level} to the Audit log.
   *
   * @param level describing the severity of the log.
   * @param entry the {@link AuditLogEntry} to push to the audit log.
   */
  private static void log(Level level, AuditLogEntry entry) {
    // Check if audit logging is of this specific log entry enabled.
    if (!properties.isEnabled()) {
      return;
    }
    if (entry.getKind().equals(AuditLogEntryKind.MESSAGE)
        && !properties.isMessageLoggingEnabled()) {
      return;
    }

    // Log event to audit log through enabled formats
    String entryJSON = entry.toJSON();
    switch (level) {
      case TRACE:
        log.trace(AUDIT_MARKER, entryJSON);
        break;
      case DEBUG:
        log.debug(AUDIT_MARKER, entryJSON);
        break;
      case INFO:
        log.info(AUDIT_MARKER, entryJSON);
        break;
      case WARN:
        log.warn(AUDIT_MARKER, entryJSON);
        break;
      case ERROR:
        log.error(AUDIT_MARKER, entryJSON);
        break;
    }
  }
}
