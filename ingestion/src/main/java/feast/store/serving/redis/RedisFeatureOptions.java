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

package feast.store.serving.redis;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import feast.options.Options;
import feast.options.Validation.ISO8601Duration;
import org.joda.time.Duration;
import org.joda.time.format.ISOPeriodFormat;

public class RedisFeatureOptions implements Options {

  static final String DEFAULT_EXPIRY = "PT0H";

  @ISO8601Duration
  @JsonProperty(value = "redis.expiry")
  public String expiry = DEFAULT_EXPIRY; // ISO8601 Period

  @JsonIgnore
  Duration getExpiryDuration() {
    return ISOPeriodFormat.standard().parsePeriod(expiry).toStandardDuration();
  }
}
