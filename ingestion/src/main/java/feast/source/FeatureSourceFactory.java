/*
 * Copyright 2019 The Feast Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package feast.source;

import feast.specs.ImportJobSpecsProto.SourceSpec;
import feast.specs.ImportJobSpecsProto.SourceSpec.SourceType;
import java.util.List;

/**
 * A FeatureSourceFactory creates FeatureSource instances, which can read FeatureRow messages from a
 * source location.
 */
public interface FeatureSourceFactory {

  SourceType getType();

  FeatureSource create(SourceSpec sourceSpec, List<String> featureIds);
}
