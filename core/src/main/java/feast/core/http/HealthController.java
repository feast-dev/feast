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
package feast.core.http;

import static org.springframework.http.HttpStatus.INTERNAL_SERVER_ERROR;

import java.sql.Connection;
import java.sql.SQLException;
import javax.sql.DataSource;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

/** Web http for pod health-check endpoints. */
@Slf4j
@RestController
public class HealthController {

  private final DataSource db;

  @Autowired
  public HealthController(DataSource datasource) {
    this.db = datasource;
  }

  /**
   * /ping endpoint checks if the application is ready to serve traffic by checking if it is able to
   * access the metadata db.
   */
  @RequestMapping(value = "/ping", method = RequestMethod.GET)
  public ResponseEntity ping() {
    return ResponseEntity.ok("pong");
  }

  /**
   * /healthz endpoint checks if the application is healthy by checking if the application still has
   * access to the metadata db.
   */
  @RequestMapping(value = "/healthz", method = RequestMethod.GET)
  public ResponseEntity healthz() {
    try (Connection conn = db.getConnection()) {
      if (conn.isValid(10)) {
        return ResponseEntity.ok("healthy");
      }
      log.error("Unable to reach DB");
      return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
          .body("Unable to establish connection with DB");
    } catch (SQLException e) {
      log.error("Unable to reach DB: {}", e);
      return ResponseEntity.status(INTERNAL_SERVER_ERROR).body(e.getMessage());
    }
  }
}
