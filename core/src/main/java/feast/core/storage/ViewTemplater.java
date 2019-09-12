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

import com.github.mustachejava.DefaultMustacheFactory;
import com.github.mustachejava.Mustache;
import com.github.mustachejava.MustacheFactory;

import java.io.StringReader;
import java.io.StringWriter;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Generates the query for creation or update of a warehouse view
 */
public class ViewTemplater {
  private final Mustache template;

  public ViewTemplater(String templateString, String templateName) {
    MustacheFactory mf = new DefaultMustacheFactory();
    this.template = mf.compile(new StringReader(templateString), templateName);
  }

  static class TemplateValues {
    String tableName;
    String tableId;
    List<Feature> features;

    TemplateValues(String tableId, List<String> features) {
      this.tableId = tableId;
      this.tableName = tableId.substring(tableId.lastIndexOf(".") + 1);
      this.features = features.stream().map(Feature::new).collect(Collectors.toList());
    }
  }

  static class Feature {
    String name;

    Feature(String name) {
      this.name = name;
    }
  }

  /**
   * Get a query for building or updating the warehouse view given a set of parameters
   * @param tableId Table ID to update, including table name, dataset name, and project id (for BigQuery)
   * @param features List of features to include in view
   * @return Warehouse view creation query string
   */
  public String getViewQuery(String tableId, List<String> features) {
    TemplateValues values = new TemplateValues(tableId, features);
    StringWriter writer = new StringWriter();
    template.execute(writer, values);
    return writer.toString();
  }
}
