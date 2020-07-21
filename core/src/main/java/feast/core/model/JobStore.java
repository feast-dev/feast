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
package feast.core.model;

import com.google.common.base.Objects;
import com.google.protobuf.InvalidProtocolBufferException;
import feast.proto.core.StoreProto;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.Serializable;
import javax.persistence.*;
import javax.persistence.Entity;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;

/**
 * Represents {@link Store}s attached to one {@link Job}. Keeps copy of Store's proto to detect
 * changes in original Store.
 */
@Entity
@Table(
    name = "jobs_stores",
    indexes = {
      @Index(name = "idx_jobs_stores_job_id", columnList = "job_id"),
      @Index(name = "idx_jobs_stores_store_name", columnList = "store_name")
    })
@Getter
@Setter
public class JobStore {
  @Embeddable
  @EqualsAndHashCode
  @AllArgsConstructor
  public static class JobStoreKey implements Serializable {
    public JobStoreKey() {}

    @Column(name = "job_id")
    String jobId;

    @Column(name = "store_name")
    String storeName;
  }

  @EmbeddedId private JobStoreKey id = new JobStoreKey();

  @ManyToOne
  @MapsId("jobId")
  @JoinColumn(name = "job_id")
  private Job job;

  @Column(name = "store_proto", nullable = false)
  @Lob
  private byte[] storeProto;

  public JobStore() {}

  public JobStore(Job job, Store store) {
    this.job = job;
    this.id.storeName = store.getName();
    try {
      setStoreProto(store.toProto());
    } catch (InvalidProtocolBufferException e) {
      throw new RuntimeException("Couldn't convert Store to proto. Reason: %s", e.getCause());
    }
  }

  public StoreProto.Store getStoreProto() {
    try {
      return StoreProto.Store.parseFrom(this.storeProto);
    } catch (InvalidProtocolBufferException e) {
      return StoreProto.Store.newBuilder().build();
    }
  }

  public void setStoreProto(StoreProto.Store storeProto) {
    ByteArrayOutputStream output = new ByteArrayOutputStream();
    try {
      storeProto.writeTo(output);
    } catch (IOException e) {
      throw new RuntimeException(
          String.format("Couldn't write StoreProto to byteArray: %s", e.getCause()));
    }

    this.storeProto = output.toByteArray();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    JobStore jobStore = (JobStore) o;
    return Objects.equal(this.id, jobStore.id)
        && Objects.equal(this.storeProto, jobStore.storeProto);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(this.id, this.storeProto);
  }
}
