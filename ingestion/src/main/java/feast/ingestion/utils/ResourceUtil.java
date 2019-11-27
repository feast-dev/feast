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
package feast.ingestion.utils;

import com.google.common.io.Resources;
import java.nio.charset.StandardCharsets;
import org.slf4j.Logger;

public class ResourceUtil {
  private static final String DEADLETTER_SCHEMA_FILE_PATH = "schemas/deadletter_table_schema.json";
  private static final Logger log = org.slf4j.LoggerFactory.getLogger(ResourceUtil.class);

  public static String getDeadletterTableSchemaJson() {
    String schemaJson = null;
    try {
      schemaJson =
          Resources.toString(
              Resources.getResource(DEADLETTER_SCHEMA_FILE_PATH), StandardCharsets.UTF_8);
    } catch (Exception e) {
      log.error(
          "Unable to read {} file from the resources folder!", DEADLETTER_SCHEMA_FILE_PATH, e);
    }
    return schemaJson;
  }
}
