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
package feast.core.dao;

import feast.core.model.FeatureSet;
import java.util.List;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;

/** JPA repository supplying FeatureSet objects keyed by id. */
public interface FeatureSetRepository extends JpaRepository<FeatureSet, String> {

  long count();

  // Find feature set by name and version
  FeatureSet findFeatureSetByNameAndVersion(String name, Integer version);

  // Find latest version of a feature set by name
  FeatureSet findFirstFeatureSetByNameOrderByVersionDesc(String name);

  // find all versions of featureSets matching the given name.
  List<FeatureSet> findByName(String name);

  // find all versions of featureSets with names matching the regex
  @Query(nativeQuery = true, value = "SELECT * FROM feature_sets WHERE name LIKE ?1")
  List<FeatureSet> findByNameWithWildcard(String name);
}
