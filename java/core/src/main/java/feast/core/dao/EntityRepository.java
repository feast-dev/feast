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

import feast.core.model.EntityV2;
import java.util.List;
import org.springframework.data.jpa.repository.JpaRepository;

/** JPA repository supplying EntityV2 objects keyed by id. */
public interface EntityRepository extends JpaRepository<EntityV2, String> {

  long count();

  // Find all EntityV2s by project
  List<EntityV2> findAllByProject_Name(String project);

  // Find single EntityV2 by project and name
  EntityV2 findEntityByNameAndProject_Name(String name, String project);
}
