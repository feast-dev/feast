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
package feast.core.dao;

import feast.core.model.FeatureTable;
import java.util.List;
import java.util.Optional;
import org.springframework.data.jpa.repository.JpaRepository;

/** JPA repository for querying FeatureTables stored. */
public interface FeatureTableRepository extends JpaRepository<FeatureTable, Long> {
  // Find single FeatureTable by project and name
  Optional<FeatureTable> findFeatureTableByNameAndProject_Name(String name, String projectName);

  // Find FeatureTables by project
  List<FeatureTable> findAllByProject_Name(String projectName);
}
