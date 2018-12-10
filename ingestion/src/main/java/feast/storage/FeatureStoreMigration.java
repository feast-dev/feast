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

package feast.storage;

import feast.specs.FeatureSpecProto.FeatureSpec;
import java.util.List;
import lombok.Value;

/**
 * This class is for notifying storage plugins about new or changed FeatureSpecs so they can update
 * appropriate schemas.
 */
@Value
public class FeatureStoreMigration {
  List<FeatureSpec> create;
  List<FeatureSpec> drop;
  List<FeatureSpec> update;
}
