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
package feast.common.logging.entry;

import com.google.gson.Gson;

/**
 * AuditLogEntry represents a single audit Log Entry. Audit log entry can converted into string with
 * {{@link #toString()} for human readable representation. Or structured JSON with {{@link
 * #toJSON()} for a machine parsable representation.
 */
public abstract class AuditLogEntry {
  /** Declare Log Type to allow external Logging systems to filter out {@link AuditLogEntry} */
  public final String logType = "FeastAuditLogEntry";

  public final String application = "Feast";

  /**
   * The name of the Feast component producing this {@link AuditLogEntry}
   *
   * @return the component
   */
  public abstract String getComponent();

  /**
   * The version of Feast producing this {@link AuditLogEntry}
   *
   * @return version
   */
  public abstract String getVersion();

  public abstract AuditLogEntryKind getKind();

  /**
   * Return a structured JSON representation of this {@link AuditLogEntry}
   *
   * @return structured JSON representation
   */
  public String toJSON() {
    Gson gson = new Gson();
    return gson.toJson(this);
  }
}
