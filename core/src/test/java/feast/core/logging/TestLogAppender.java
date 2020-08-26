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
package feast.core.logging;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import lombok.Getter;
import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.Core;
import org.apache.logging.log4j.core.Filter;
import org.apache.logging.log4j.core.Layout;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Property;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.config.plugins.PluginAttribute;
import org.apache.logging.log4j.core.config.plugins.PluginElement;
import org.apache.logging.log4j.core.config.plugins.PluginFactory;
import org.apache.logging.log4j.core.layout.PatternLayout;

/** Test Log Appender used for collecting logs for testing logging. */
@Plugin(
    name = "TestLogAppender",
    category = Core.CATEGORY_NAME,
    elementType = Appender.ELEMENT_TYPE)
@Getter
public class TestLogAppender extends AbstractAppender {
  private List<String> logs;

  protected TestLogAppender(String name, Filter filter, Layout<? extends Serializable> layout) {
    super(name, filter, layout, false, new Property[] {});
    logs = new ArrayList<>();
  }

  @Override
  public void append(LogEvent event) {
    getLogs().add(event.getMessage().toString());
  }

  @PluginFactory
  public static TestLogAppender createAppender(
      @PluginAttribute("name") String name,
      @PluginElement("Layout") Layout<? extends Serializable> layout,
      @PluginElement("Filter") final Filter filter) {
    if (name == null) {
      return null;
    }
    if (layout == null) {
      layout = PatternLayout.createDefaultLayout();
    }
    return new TestLogAppender(name, filter, layout);
  }
}
