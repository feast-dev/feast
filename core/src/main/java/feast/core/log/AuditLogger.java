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
package feast.core.log;

import com.google.common.base.Strings;
import java.util.Date;
import java.util.Map;
import java.util.TreeMap;
import lombok.extern.log4j.Log4j2;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.message.ObjectMessage;

@Log4j2
public class AuditLogger {
  private static final Level AUDIT_LEVEL = Level.getLevel("AUDIT");

  /**
   * Log to stdout a json formatted audit log.
   *
   * @param resource type of resource
   * @param id id of resource, if any
   * @param action action taken
   * @param detail additional detail. Supports string formatting.
   * @param args arguments to the detail string
   */
  public static void log(
      Resource resource, String id, Action action, String detail, Object... args) {
    Map<String, String> map = new TreeMap<>();
    map.put("timestamp", new Date().toString());
    map.put("resource", resource.toString());
    map.put("id", id);
    map.put("action", action.toString());
    map.put("detail", Strings.lenientFormat(detail, args));
    ObjectMessage msg = new ObjectMessage(map);

    log.log(AUDIT_LEVEL, msg);
  }
}
