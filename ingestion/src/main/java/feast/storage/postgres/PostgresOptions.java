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

package feast.storage.postgres;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.io.Serializable;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Pattern;
import feast.options.Options;

public class PostgresOptions implements Options, Serializable {
  private static final String OPT_POSTGRES_URL = "postgres.url";
  private static final String OPT_POSTGRES_PREFIX = "postgres.prefix";

  @JsonProperty(value = OPT_POSTGRES_URL)
  @NotNull
  @Pattern(regexp = "jdbc:postgresql:.+")
  String url;

  @JsonProperty(value = OPT_POSTGRES_PREFIX)
  @Pattern(regexp = "[a-zA-Z_][a-zA-Z0-9_]*")
  String prefix;
}
