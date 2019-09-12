/*
 * Copyright 2018 The Feast Authors
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
 *
 */

package feast.core.storage;

import org.junit.Test;

import java.util.Arrays;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

public class ViewTemplaterTest {
  @Test
  public void shouldExecuteTemplateGivenTemplateValues() {
    String testTemplate =
            "{{tableId}}.{{tableName}}{{#features}}.{{name}}{{/features}}";
    ViewTemplater templater = new ViewTemplater(testTemplate, "testView");
    String out = templater.getViewQuery("p.ds.tn", Arrays.asList("f1", "f2"));
    assertThat(out, equalTo("p.ds.tn.tn.f1.f2"));
  }
}
