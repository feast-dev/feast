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
package feast.store.serving.cassandra;

import com.datastax.driver.mapping.annotations.ClusteringColumn;
import com.datastax.driver.mapping.annotations.Computed;
import com.datastax.driver.mapping.annotations.PartitionKey;
import com.datastax.driver.mapping.annotations.Table;
import feast.core.FeatureSetProto.EntitySpec;
import feast.core.FeatureSetProto.FeatureSetSpec;
import feast.ingestion.utils.ValueUtil;
import feast.types.FeatureRowProto.FeatureRow;
import feast.types.FieldProto.Field;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;

/**
 * Cassandra's object mapper that handles basic CRUD operations in Cassandra tables More info:
 * https://docs.datastax.com/en/developer/java-driver/3.1/manual/object_mapper/
 */
@DefaultCoder(value = AvroCoder.class)
@Table(name = "feature_store")
public final class CassandraMutation implements Serializable {

  public static final String ENTITIES = "entities";
  public static final String FEATURE = "feature";
  public static final String VALUE = "value";

  @PartitionKey private final String entities;

  @ClusteringColumn private final String feature;

  private final ByteBuffer value;

  @Computed(value = "writetime(value)")
  private final long writeTime;

  @Computed(value = "ttl(value)")
  private final int ttl;

  // NoArgs constructor is needed when using Beam's CassandraIO withEntity and specifying this
  // class,
  // it looks for an init() method
  CassandraMutation() {
    this.entities = null;
    this.feature = null;
    this.value = null;
    this.writeTime = 0;
    this.ttl = 0;
  }

  CassandraMutation(String entities, String feature, ByteBuffer value, long writeTime, int ttl) {
    this.entities = entities;
    this.feature = feature;
    this.value = value;
    this.writeTime = writeTime;
    this.ttl = ttl;
  }

  public long getWriteTime() {
    return writeTime;
  }

  public int getTtl() {
    return ttl;
  }

  static String keyFromFeatureRow(FeatureSetSpec featureSetSpec, FeatureRow featureRow) {
    Set<String> entityNames =
        featureSetSpec.getEntitiesList().stream()
            .map(EntitySpec::getName)
            .collect(Collectors.toSet());
    List<Field> entities = new ArrayList<>();
    for (Field field : featureRow.getFieldsList()) {
      if (entityNames.contains(field.getName())) {
        entities.add(field);
      }
    }
    return featureRow.getFeatureSet()
        + ":"
        + entities.stream()
            .map(f -> f.getName() + "=" + ValueUtil.toString(f.getValue()))
            .collect(Collectors.joining("|"));
  }

  @Override
  public boolean equals(Object o) {
    if (o == this) {
      return true;
    }
    if (o instanceof CassandraMutation) {
      CassandraMutation that = (CassandraMutation) o;
      return this.entities.equals(that.entities)
          && this.feature.equals(that.feature)
          && this.value.equals(that.value)
          && this.writeTime == that.writeTime
          && this.ttl == that.ttl;
    }
    return false;
  }
}
