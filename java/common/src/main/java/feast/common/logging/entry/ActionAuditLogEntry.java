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
package feast.common.logging.entry;

import com.google.auto.value.AutoValue;

/** ActionAuditLogEntry records an action being taken on a specific resource */
@AutoValue
public abstract class ActionAuditLogEntry extends AuditLogEntry {
  /** @return The name of the action taken on the resource. */
  public abstract String getAction();

  /** @return The target resource of which the action was taken on. */
  public abstract LogResource getResource();

  /**
   * Create an {@link AuditLogEntry} that records an action being taken on a specific resource.
   *
   * @param component The name of th Feast component producing this {@link AuditLogEntry}.
   * @param version The version of Feast producing this {@link AuditLogEntry}.
   * @param resource The target resource of which the action was taken on.
   * @param action The name of the action being taken on the given resource.
   * @return log entry that records an action being taken on a specific resource
   */
  public static ActionAuditLogEntry of(
      String component, String version, LogResource resource, String action) {
    return new AutoValue_ActionAuditLogEntry(
        component, version, AuditLogEntryKind.ACTION, action, resource);
  }
}
