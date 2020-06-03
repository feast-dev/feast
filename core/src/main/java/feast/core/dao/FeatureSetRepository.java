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

/** JPA repository supplying FeatureSet objects keyed by id. */
public interface FeatureSetRepository extends JpaRepository<FeatureSet, String> {

  long count();

  // Find single feature set by project and name
  FeatureSet findFeatureSetByNameAndProject_Name(String name, String project);

  // find all feature sets and order by name
  List<FeatureSet> findAllByOrderByNameAsc();

  // find all feature sets matching the given name pattern with a specific project.
  List<FeatureSet> findAllByNameLikeAndProject_NameOrderByNameAsc(String name, String project_name);

  // find all feature sets matching the given name pattern and project pattern
  List<FeatureSet> findAllByNameLikeAndProject_NameLikeOrderByNameAsc(
      String name, String project_name);
}
