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

import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import feast.serving.ServingAPIProto.Job;
import feast.serving.ServingAPIProto.Job.Builder;
import java.util.Date;
import java.util.Optional;
import org.joda.time.Duration;
import org.slf4j.Logger;

// TODO: Do rate limiting, currently if clients call get() or upsert()
//       and an exceedingly high rate e.g. they wrap job reload in a while loop with almost no wait
//       Redis connection may break and need to restart Feast serving. Need to handle this.

public class CassandraBackedJobService implements JobService {

  private static final Logger log =
      org.slf4j.LoggerFactory.getLogger(CassandraBackedJobService.class);
  private final Session session;
  private final int defaultExpirySeconds = (int) Duration.standardDays(1).getStandardSeconds();

  public CassandraBackedJobService(Session session) {
    this.session = session;
  }

  @Override
  public Optional<Job> get(String id) {
    Job job = null;
    Job latestJob = Job.newBuilder().build();
    ResultSet res =
        session.execute(
            QueryBuilder.select()
                .column("job_uuid")
                .column("job_data")
                .column("timestamp")
                .from("admin", "jobs")
                .where(QueryBuilder.eq("job_uuid", id)));
    Date timestamp = new Date(0);
    while (!res.isExhausted()) {
      Row r = res.one();
      ColumnDefinitions defs = r.getColumnDefinitions();
      Date newTs = new Date(0);
      for (int i = 0; i < defs.size(); i++) {
        if (defs.getName(i).equals("timestamp")) {
          if (r.getTimestamp(i).compareTo(timestamp) > 0) {
            newTs = r.getTimestamp(i);
          }
        }
        if (defs.getName(i).equals("job_data")) {
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
      session.execute(
          QueryBuilder.insertInto("admin", "jobs")
              .value("job_uuid", job.getId())
              .value("timestamp", System.currentTimeMillis())
              .value("job_data", JsonFormat.printer().omittingInsignificantWhitespace().print(job))
              .using(QueryBuilder.ttl(defaultExpirySeconds)));
    } catch (Exception e) {
      log.error(String.format("Failed to upsert job: %s", e.getMessage()));
    }
  }
}
