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
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import java.net.URI;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.SpringBootTest.WebEnvironment;
import org.springframework.http.MediaType;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.web.util.UriComponentsBuilder;

@RunWith(SpringRunner.class)
@SpringBootTest(webEnvironment = WebEnvironment.RANDOM_PORT)
@AutoConfigureMockMvc
public class CoreServiceRestControllerTest {

  @Autowired private MockMvc mockMvc;
  private static String UrlPrefix = "/api/v1";

  @Test
  public void getVersion() throws Exception {
    mockMvc
        .perform(
            MockMvcRequestBuilders.get(UrlPrefix + "/version").accept(MediaType.APPLICATION_JSON))
        .andDo(print())
        .andExpect(status().isOk())
        .andExpect(content().contentType("application/json"))
        .andExpect(jsonPath("$.version").isNotEmpty());
  }

  @Test
  public void listProjects() throws Exception {
    mockMvc
        .perform(
            MockMvcRequestBuilders.get(UrlPrefix + "/projects").accept(MediaType.APPLICATION_JSON))
        .andDo(print())
        .andExpect(status().isOk())
        .andExpect(content().contentType("application/json"))
        .andExpect(jsonPath("$.projects").isNotEmpty());
    ;
  }

  @Test
  public void listFeatureSets() throws Exception {
    URI uri =
        UriComponentsBuilder.fromPath(UrlPrefix)
            .path("/feature-sets")
            .queryParam("project", "default")
            .build()
            .toUri();
    mockMvc
        .perform(MockMvcRequestBuilders.get(uri).accept(MediaType.APPLICATION_JSON))
        .andDo(print())
        .andExpect(status().isOk())
        .andExpect(content().contentType("application/json"));
    ;
  }

  @Test
  public void listFeatures() throws Exception {
    URI uri =
        UriComponentsBuilder.fromPath(UrlPrefix)
            .path("/features")
            .queryParam("entities", "")
            .build()
            .toUri();
    mockMvc
        .perform(MockMvcRequestBuilders.get(uri).accept(MediaType.APPLICATION_JSON))
        .andDo(print())
        .andExpect(status().isOk())
        .andExpect(content().contentType("application/json"));
  }
}
