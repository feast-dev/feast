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
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.google.gson.JsonObject;
import com.google.protobuf.Any;
import com.google.protobuf.MapEntry;
import com.google.protobuf.util.JsonFormat;
import com.datastax.driver.core.Session;
import feast.serving.ServingAPIProto.Job;
import feast.serving.ServingAPIProto.Job.Builder;
import org.joda.time.Duration;
import org.joda.time.MutableDateTime;
import org.slf4j.Logger;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.exceptions.JedisConnectionException;

import javax.xml.crypto.Data;
import java.util.List;
import java.util.Map;
import java.util.Optional;

// TODO: Do rate limiting, currently if clients call get() or upsert()
//       and an exceedingly high rate e.g. they wrap job reload in a while loop with almost no wait
//       Redis connection may break and need to restart Feast serving. Need to handle this.

public class CassandraBackedJobService implements JobService {

  private static final Logger log = org.slf4j.LoggerFactory.getLogger(CassandraBackedJobService.class);
  private final Session session;
  // Remove job state info after "defaultExpirySeconds" to prevent filling up Redis memory
  // and since users normally don't require info about relatively old jobs.
  private final int defaultExpirySeconds = (int) Duration.standardDays(1).getStandardSeconds();

  public CassandraBackedJobService(Session session) {
    this.session = session;
  }

  @Override
  public Optional<Job> get(String id) {
    Job job = null;
    try {
      ResultSet res =  session.execute(
              QueryBuilder.select()
                      .column("job_uuid")
                      .column("value")
                      .writeTime("value")
                      .as("writetime")
                      .from("admin", "jobs")
                      .where(QueryBuilder.eq("job_uuid", id)));
      JsonObject result = new JsonObject();
      Builder builder = Job.newBuilder();
      while (!res.isExhausted()) {
        Row r = res.one();
        ColumnDefinitions defs = r.getColumnDefinitions();
        for (int i = 0; i < defs.size(); i++) {
          result.addProperty(defs.getName(i), r.getString(i));
        }
      }
      if (result == null) {
        return Optional.empty();
      }
      JsonFormat.parser().merge(result.toString(), builder);
      job = builder.build();
    } catch (Exception e) {
      log.error(String.format("Failed to parse JSON for Feast job: %s", e.getMessage()));
    }
    return Optional.ofNullable(job);
  }

  @Override
  public void upsert(Job job) {
    try {
      session.execute(QueryBuilder.update(job.getId(), JsonFormat.printer().omittingInsignificantWhitespace().print(job)));
    } catch (Exception e) {
      log.error(String.format("Failed to upsert job: %s", e.getMessage()));
    }
  }
}
