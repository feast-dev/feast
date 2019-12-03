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

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

import java.sql.Connection;
import java.sql.SQLException;
import javax.sql.DataSource;
import org.junit.Test;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

public class HealthControllerTest {
  @Test
  public void ping() {
    HealthController healthController = new HealthController(null);
    assertEquals(ResponseEntity.ok("pong"), healthController.ping());
  }

  @Test
  public void healthz() {
    assertEquals(ResponseEntity.ok("healthy"), mockHealthyController().healthz());
    assertEquals(
        ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
            .body("Unable to establish connection with DB"),
        mockUnhealthyControllerBecauseInvalidConn().healthz());
    assertEquals(
        ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body("mocked sqlexception"),
        mockUnhealthyControllerBecauseSQLException().healthz());
  }

  private HealthController mockHealthyController() {
    DataSource mockDataSource = mock(DataSource.class);
    Connection mockConnection = mock(Connection.class);
    try {
      when(mockConnection.isValid(any(int.class))).thenReturn(Boolean.TRUE);
      when(mockDataSource.getConnection()).thenReturn(mockConnection);
    } catch (Exception e) {
      e.printStackTrace();
    }
    return new HealthController(mockDataSource);
  }

  private HealthController mockUnhealthyControllerBecauseInvalidConn() {
    DataSource mockDataSource = mock(DataSource.class);
    Connection mockConnection = mock(Connection.class);
    try {
      when(mockConnection.isValid(any(int.class))).thenReturn(Boolean.FALSE);
      when(mockDataSource.getConnection()).thenReturn(mockConnection);
    } catch (Exception ignored) {
    }
    return new HealthController(mockDataSource);
  }

  private HealthController mockUnhealthyControllerBecauseSQLException() {
    DataSource mockDataSource = mock(DataSource.class);
    Connection mockConnection = mock(Connection.class);
    try {
      when(mockDataSource.getConnection()).thenThrow(new SQLException("mocked sqlexception"));
    } catch (SQLException ignored) {
    }
    return new HealthController(mockDataSource);
  }
}
