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

import com.google.common.collect.Maps;
import feast.core.UIServiceProto.UIServiceTypes.FeatureGroupDetail;
import feast.core.util.TypeConversion;
import feast.specs.FeatureGroupSpecProto.FeatureGroupSpec;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

/**
 * A row in the registry storing information about a single feature group, including its relevant
 * metadata.
 */
@AllArgsConstructor
@Entity
@Getter
@Setter
@Table(name = "feature_groups")
public class FeatureGroupInfo extends AbstractTimestampEntity {

  @Id
  private String id;

  @Column(name = "tags")
  private String tags;

  @Column(name = "options")
  private String options;

  public FeatureGroupInfo() {
    super();
  }

  public FeatureGroupInfo(
      FeatureGroupSpec spec) {
    this.id = spec.getId();
    this.tags = String.join(",", spec.getTagsList());
    this.options = TypeConversion.convertMapToJsonString(spec.getOptionsMap());
  }

  /**
   * Get the feature group spec associated with this record.
   */
  public FeatureGroupSpec getFeatureGroupSpec() {
    return FeatureGroupSpec.newBuilder()
        .setId(id)
        .addAllTags(TypeConversion.convertTagStringToList(tags))
        .putAllOptions(TypeConversion.convertJsonStringToMap(options))
        .build();
  }

  /**
   * Get the feature group detail containing both spec and metadata, associated with this record.
   */
  public FeatureGroupDetail getFeatureGroupDetail() {
    return FeatureGroupDetail.newBuilder()
        .setSpec(this.getFeatureGroupSpec())
        .setLastUpdated(TypeConversion.convertTimestamp(this.getLastUpdated()))
        .build();
  }

  public void update(FeatureGroupSpec update) throws IllegalArgumentException {
    if (!isLegalUpdate(update)) {
      throw new IllegalArgumentException(
          "Feature group already exists. Update only allowed for fields: [tags]");
    }
    this.tags = String.join(",", update.getTagsList());
  }

  private boolean isLegalUpdate(FeatureGroupSpec update) {
    FeatureGroupSpec spec = this.getFeatureGroupSpec();
    return Maps.difference(spec.getOptionsMap(), update.getOptionsMap()).areEqual();
  }
}
