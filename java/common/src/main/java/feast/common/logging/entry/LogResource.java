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

import com.google.auto.value.AutoValue;

@AutoValue
/**
 * LogResource is used in {@link AuditLogEntry} to reference a specific resource as the subject of
 * the log
 */
public abstract class LogResource {
  public enum ResourceType {
    JOB,
    FEATURE_TABLE
  }

  public abstract ResourceType getType();

  public abstract String getId();

  public static LogResource of(ResourceType type, String id) {
    return new AutoValue_LogResource(type, id);
  }
}
