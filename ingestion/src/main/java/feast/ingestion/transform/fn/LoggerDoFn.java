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
package feast.ingestion.transform.fn;

import com.google.protobuf.Message;
import com.google.protobuf.util.JsonFormat;
import com.google.protobuf.util.JsonFormat.Printer;
import org.apache.beam.sdk.transforms.DoFn;
import org.slf4j.Logger;
import org.slf4j.event.Level;

public class LoggerDoFn extends DoFn<Message, Void> {

  private static final Logger log = org.slf4j.LoggerFactory.getLogger(LoggerDoFn.class);
  private Level level;
  private String prefix = "";

  public LoggerDoFn(Level level) {
    this.level = level;
  }

  public LoggerDoFn(Level level, String prefix) {
    this.level = level;
    this.prefix = prefix;
  }

  @ProcessElement
  public void processElement(ProcessContext context) {
    Printer printer = JsonFormat.printer().omittingInsignificantWhitespace();
    String message;
    try {
      message = prefix + printer.print(context.element());
    } catch (Exception e) {
      log.error(e.getMessage(), e);
      message = prefix + context.element().toString();
    }
    switch (level) {
      case INFO:
        log.info(message);
        break;
      case ERROR:
        log.error(message);
        break;
      case WARN:
        log.warn(message);
        break;
      case DEBUG:
        log.debug(message);
        break;
      case TRACE:
        log.trace(message);
        break;
    }
  }
}
