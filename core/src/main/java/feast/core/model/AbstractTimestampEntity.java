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
package feast.core.model;

import java.time.Instant;
import java.util.Date;
import javax.persistence.*;
import lombok.Data;

/**
 * Base object class ensuring that all objects stored in the registry have an auto-generated
 * creation and last updated time.
 */
@MappedSuperclass
@Data
public abstract class AbstractTimestampEntity {
  @Temporal(TemporalType.TIMESTAMP)
  @Column(name = "created", nullable = false)
  private Date created;

  @Temporal(TemporalType.TIMESTAMP)
  @Column(name = "lastUpdated", nullable = false)
  private Date lastUpdated;

  @PrePersist
  protected void onCreate() {
    lastUpdated = created = new Date();
  }

  @PreUpdate
  protected void onUpdate() {
    lastUpdated = new Date();
  }

  // This constructor is used for testing.
  public AbstractTimestampEntity() {
    this.created = Date.from(Instant.ofEpochMilli(0L));
    this.lastUpdated = Date.from(Instant.ofEpochMilli(0L));
  }
}
