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

package feast.storage.file;

import com.fasterxml.jackson.annotation.JsonIgnore;
import javax.validation.constraints.NotEmpty;
import org.joda.time.Duration;
import org.joda.time.format.ISOPeriodFormat;
import feast.options.Options;
import feast.options.Validation.ISO8601Duration;

public class FileStoreOptions implements Options {
  static final Duration DEFAULT_WINDOW_SIZE = Duration.standardMinutes(5);

  @NotEmpty public String path;

  @ISO8601Duration public String windowSize;

  @JsonIgnore public String jobName;

  @JsonIgnore
  public Duration getWindowDuration() {
    if (windowSize == null) {
      return DEFAULT_WINDOW_SIZE;
    }
    return ISOPeriodFormat.standard().parsePeriod(windowSize).toStandardDuration();
  }
}
