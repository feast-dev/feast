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

  // Find single feature set by project, name, and version
  FeatureSet findFeatureSetByNameAndProject_NameAndVersion(
      String name, String project, Integer version);

  // Find single latest version of a feature set by project and name (LIKE)
  FeatureSet findFirstFeatureSetByNameLikeAndProject_NameOrderByVersionDesc(
      String name, String project);

  // find all feature sets and order by name and version
  List<FeatureSet> findAllByOrderByNameAscVersionAsc();
  
  // find all feature sets by name and project name
  List<FeatureSet> findAllByNameAndProject_Name(String name, String projectName);
  
  // find all feature sets by name and version
  List<FeatureSet> findAllByNameAndVersion(String name, Integer version);
  
  // find all feature sets within a project and order by name and version
  List<FeatureSet> findAllByProject_NameOrderByNameAscVersionAsc(String project_name);

  // find all versions of feature sets matching the given name pattern with a specific project.
  List<FeatureSet> findAllByNameLikeAndProject_NameOrderByNameAscVersionAsc(
      String name, String project_name);

  // find all versions of feature sets matching the given name pattern and project pattern
  List<FeatureSet> findAllByNameLikeAndProject_NameLikeOrderByNameAscVersionAsc(
      String name, String project_name);
}
