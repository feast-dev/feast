package feast.serving.service;

import com.google.protobuf.util.JsonFormat;
import feast.serving.ServingAPIProto.Job;
import feast.serving.ServingAPIProto.Job.Builder;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.joda.time.Duration;
import redis.clients.jedis.Jedis;

// TODO: Do rate limiting, currently if clients call get() or upsert()
//       and an exceedingly high rate e.g. they wrap job reload in a while loop with almost no wait
//       Redis connection may break and need to restart Feast serving. Need to handle this.

@Slf4j
public class RedisBackedJobService implements JobService {
  private final Jedis jedis;
  // Remove job state info after "defaultExpirySeconds" to prevent filling up Redis memory
  // and since users normally don't require info about relatively old jobs.
  private final int defaultExpirySeconds = (int) Duration.standardDays(1).getStandardSeconds();

  public RedisBackedJobService(Jedis jedis) {
    this.jedis = jedis;
  }

  @Override
  public Optional<Job> get(String id) {
    String json = jedis.get(id);
    if (json == null) {
      return Optional.empty();
    }
    Job job = null;
    Builder builder = Job.newBuilder();
    try {
      JsonFormat.parser().merge(json, builder);
      job = builder.build();
    } catch (Exception e) {
      log.error(String.format("Failed to parse JSON for Feast job: %s", e.getMessage()));
    }

    return Optional.ofNullable(job);
  }

  @Override
  public void upsert(Job job) {
    try {
      jedis.set(job.getId(), JsonFormat.printer().omittingInsignificantWhitespace().print(job));
      jedis.expire(job.getId(), defaultExpirySeconds);
    } catch (Exception e) {
      log.error(String.format("Failed to upsert job: %s", e.getMessage()));
    }
  }
}
