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
package feast.core.controller;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.doReturn;

import feast.core.config.FeastProperties;
import feast.core.service.AccessManagementService;
import feast.core.service.JobService;
import feast.core.service.SpecService;
import feast.core.service.StatsService;
import feast.proto.core.CoreServiceProto.GetFeastCoreVersionResponse;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class CoreServiceRestControllerTest {

  @Mock FeastProperties feastProperties;
  @Mock SpecService specService;
  @Mock JobService jobService;
  @Mock StatsService statsService;
  @Mock AccessManagementService accessManagementService;

  @InjectMocks CoreServiceRestController controller;

  @Test
  public void getVersion() {
    String version = "v0.7";
    GetFeastCoreVersionResponse response =
        GetFeastCoreVersionResponse.newBuilder().setVersion(version).build();
    doReturn(version).when(feastProperties).getVersion();
    assertEquals(response, controller.getVersion());
  }

  @Test
  public void listProjects() {
    String version = "v0.7";
    GetFeastCoreVersionResponse response =
        GetFeastCoreVersionResponse.newBuilder().setVersion(version).build();
    doReturn(version).when(feastProperties).getVersion();
    assertEquals(response, controller.getVersion());
  }
}
