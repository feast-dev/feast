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

import static org.springframework.test.web.servlet.result.MockMvcResultHandlers.print;

import feast.core.it.BaseIT;
import org.junit.Assert;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.web.context.WebApplicationContext;
import org.springframework.web.util.UriComponentsBuilder;

@SpringBootTest
@AutoConfigureMockMvc
public class CoreServiceRestIT extends BaseIT {

  @Autowired private WebApplicationContext webApplicationContext;
  @Autowired private MockMvc mockMvc;

  @Test
  public void getVersion() throws Exception {
    Assert.assertNotNull(mockMvc);
    String url = UriComponentsBuilder.fromPath("/api/v1/version").buildAndExpand().toString();
    mockMvc
        .perform(MockMvcRequestBuilders.get(url).accept(MediaType.APPLICATION_JSON))
        .andDo(print());
  }

  @TestConfiguration
  public static class TestConfig extends BaseTestConfig {}
}
