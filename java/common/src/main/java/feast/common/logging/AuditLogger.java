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

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import feast.common.logging.config.LoggingProperties;
import feast.common.logging.config.LoggingProperties.AuditLogProperties;
import feast.common.logging.entry.*;
import feast.common.logging.entry.LogResource.ResourceType;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.fluentd.logger.FluentLogger;
import org.slf4j.Marker;
import org.slf4j.MarkerFactory;
import org.slf4j.event.Level;

@Slf4j
public class AuditLogger {
  private static final String FLUENTD_DESTINATION = "fluentd";
  private static final Marker AUDIT_MARKER = MarkerFactory.getMarker("AUDIT_MARK");
  private static FluentLogger fluentLogger;
  private static AuditLogProperties properties;
  private static String artifact;
  private static String version;

  public AuditLogger(LoggingProperties loggingProperties, String artifact, String version) {
    // Spring runs this constructor when creating the AuditLogger bean,
    // which allows us to populate the AuditLogger class with dependencies.
    // This allows us to use the dependencies in the AuditLogger's static methods
    AuditLogger.properties = loggingProperties.getAudit();
    AuditLogger.artifact = artifact;
    AuditLogger.version = version;
    if (AuditLogger.properties.getMessageLogging() != null
        && AuditLogger.properties.getMessageLogging().isEnabled()) {
      AuditLogger.fluentLogger =
          FluentLogger.getLogger(
              "feast",
              AuditLogger.properties.getMessageLogging().getFluentdHost(),
              AuditLogger.properties.getMessageLogging().getFluentdPort());
    }
  }

  /**
   * Log the handling of a Protobuf message by a service call.
   *
   * @param level log level
   * @param entryBuilder with all fields set except instance.
   */
  public static void logMessage(Level level, MessageAuditLogEntry.Builder entryBuilder) {
    log(level, entryBuilder.setComponent(artifact).setVersion(version).build());
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
            artifact, version, LogResource.of(resourceType, resourceId), action));
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
            artifact, version, LogResource.of(resourceType, resourceId), status));
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

    // Either forward log to logging layer or log to console
    String destination = properties.getMessageLogging().getDestination();
    if (destination.equals(FLUENTD_DESTINATION)) {
      if (entry.getKind() == AuditLogEntryKind.MESSAGE) {
        Map<String, Object> fluentdLogs = new HashMap<>();
        MessageAuditLogEntry messageAuditLogEntry = (MessageAuditLogEntry) entry;
        String releaseName;

        try {
          releaseName =
              StringUtils.defaultIfEmpty(
                  System.getenv("RELEASE_NAME"), InetAddress.getLocalHost().getHostAddress());
        } catch (UnknownHostException e) {
          releaseName = StringUtils.defaultIfEmpty(System.getenv("RELEASE_NAME"), "");
        }

        fluentdLogs.put("id", messageAuditLogEntry.getId());
        fluentdLogs.put("identity", messageAuditLogEntry.getIdentity());
        fluentdLogs.put("service", messageAuditLogEntry.getService());
        fluentdLogs.put("status_code", messageAuditLogEntry.getStatusCode());
        fluentdLogs.put("method", messageAuditLogEntry.getMethod());
        fluentdLogs.put("release_name", releaseName);
        try {
          fluentdLogs.put("request", JsonFormat.printer().print(messageAuditLogEntry.getRequest()));
          fluentdLogs.put(
              "response", JsonFormat.printer().print(messageAuditLogEntry.getResponse()));
        } catch (InvalidProtocolBufferException e) {
        }
        fluentLogger.log("fluentd", fluentdLogs);
      }
    } else {
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
}
