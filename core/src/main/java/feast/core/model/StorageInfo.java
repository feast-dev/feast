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

package feast.core.model;

import feast.core.UIServiceProto.UIServiceTypes.StorageDetail;
import feast.specs.StorageSpecProto.StorageSpec;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

import static feast.core.util.TypeConversion.*;

/**
 * A row in the registry storing information about a single storage specification, including its
 * relevant metadata.
 */
@AllArgsConstructor
@Getter
@Setter
@Entity
@Table(name = "storage")
public class StorageInfo extends AbstractTimestampEntity {

  @Id
  private String id;

  @Column(name = "type", nullable = false)
  private String type;

  @Column(name = "options")
  private String options;

  public StorageInfo() {
    super();
  }

  public StorageInfo(StorageSpec spec) {
    this.id = spec.getId();
    this.type = spec.getType();
    this.options = convertMapToJsonString(spec.getOptionsMap());
  }

  /**
   * Get the storage spec associated with this record.
   */
  public StorageSpec getStorageSpec() {
    return StorageSpec.newBuilder()
            .setId(id)
            .setType(type)
            .putAllOptions(convertJsonStringToMap(options))
            .build();
  }

  /**
   * Get the storage detail containing both spec and metadata, associated with this record.
   */
  public StorageDetail getStorageDetail() {
    return StorageDetail.newBuilder()
            .setSpec(this.getStorageSpec())
            .setLastUpdated(convertTimestamp(this.getLastUpdated()))
            .build();
  }
}
