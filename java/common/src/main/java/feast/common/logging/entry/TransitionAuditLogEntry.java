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

/** TransitionAuditLogEntry records a transition in state/status in a specific resource. */
@AutoValue
public abstract class TransitionAuditLogEntry extends AuditLogEntry {
  /** @return The resource which the state/status transition occured. */
  public abstract LogResource getResource();

  /** @return The end status with the resource transition to. */
  public abstract String getStatus();

  /**
   * Construct a new {@link AuditLogEntry} to record a transition in state/status in a specific
   * resource.
   *
   * @param component The name of th Feast component producing this {@link AuditLogEntry}.
   * @param version The version of Feast producing this {@link AuditLogEntry}.
   * @param resource the resource which the transtion occured
   * @param status the end status which the resource transitioned to.
   * @return log entry to record a transition in state/status in a specific resource
   */
  public static TransitionAuditLogEntry of(
      String component, String version, LogResource resource, String status) {
    return new AutoValue_TransitionAuditLogEntry(
        component, version, AuditLogEntryKind.TRANSITION, resource, status);
  }
}
