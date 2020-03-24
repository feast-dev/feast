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
package feast.serving.service;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.bindMarker;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.selectFrom;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.*;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.insert.Insert;
import com.datastax.oss.driver.api.querybuilder.select.Select;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import feast.serving.ServingAPIProto.Job;
import feast.serving.ServingAPIProto.Job.Builder;
import java.time.LocalDate;
import java.util.Optional;
import org.joda.time.Duration;
import org.slf4j.Logger;

// TODO: Do rate limiting, currently if clients call get() or upsert()
//       and an exceedingly high rate e.g. they wrap job reload in a while loop with almost no wait
//       Redis connection may break and need to restart Feast serving. Need to handle this.

public class CassandraBackedJobService implements JobService {

  private static final Logger log =
      org.slf4j.LoggerFactory.getLogger(CassandraBackedJobService.class);
  private final CqlSession session;
  private final int defaultExpirySeconds = (int) Duration.standardDays(1).getStandardSeconds();

  public CassandraBackedJobService(CqlSession session) {
    this.session = session;
  }

  @Override
  public Optional<Job> get(String id) {
    Job job = null;
    Job latestJob = Job.newBuilder().build();
    Select query =
        selectFrom("admin", "jobs")
            .column("job_uuid")
            .column("job_data")
            .column("timestamp")
            .whereColumn("job_uuid")
            .isEqualTo(bindMarker());
    PreparedStatement s = session.prepare(query.build());
    ResultSet res = session.execute(s.bind(id));
    LocalDate timestamp = LocalDate.EPOCH;
    while (res.getAvailableWithoutFetching() > 0) {
      Row r = res.one();
      ColumnDefinitions defs = r.getColumnDefinitions();
      LocalDate newTs = LocalDate.EPOCH;
      for (int i = 0; i < defs.size(); i++) {
        if (defs.get(i).equals("timestamp")) {
          if (r.getLocalDate(i).compareTo(timestamp) > 0) {
            newTs = r.getLocalDate(i);
          }
        }
        if (defs.get(i).equals("job_data")) {
          Builder builder = Job.newBuilder();
          try {
            JsonFormat.parser().merge(r.getString(i), builder);
            job = builder.build();
          } catch (InvalidProtocolBufferException e) {
            throw new IllegalStateException("Could not build job from %s", e);
          }
        }
      }
      if (newTs.compareTo(timestamp) > 0) {
        latestJob = job;
      }
    }
    return Optional.ofNullable(latestJob);
  }

  @Override
  public void upsert(Job job) {
    try {
      Insert query =
          QueryBuilder.insertInto("admin", "jobs")
              .value("job_uuid", bindMarker())
              .value("timestamp", bindMarker())
              .value(
                  "job_data",
                  QueryBuilder.literal(
                      JsonFormat.printer().omittingInsignificantWhitespace().print(job)))
              .usingTtl(defaultExpirySeconds);
      PreparedStatement s = session.prepare(query.build());
      ResultSet res =
          session.execute(
              s.bind(
                  QueryBuilder.literal(job.getId()),
                  QueryBuilder.literal(System.currentTimeMillis())));
    } catch (Exception e) {
      log.error(String.format("Failed to upsert job: %s", e.getMessage()));
    }
  }
}
