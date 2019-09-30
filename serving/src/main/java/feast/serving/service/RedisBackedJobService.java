package feast.serving.service;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import feast.serving.ServingAPIProto.Job;
import feast.serving.ServingAPIProto.Job.Builder;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import redis.clients.jedis.JedisPool;

@Slf4j
public class RedisBackedJobService implements JobService {
  private final JedisPool jedisPool;

  public RedisBackedJobService(JedisPool jedisPool) {
    this.jedisPool = jedisPool;
  }

  @Override
  public Optional<Job> get(String id) {
    String json = jedisPool.getResource().get(id);
    if (json == null) {
      return Optional.empty();
    }
    Job job = null;
    Builder builder = Job.newBuilder();
    try {
      JsonFormat.parser().merge(json, builder);
      job = builder.build();
    } catch (InvalidProtocolBufferException e) {
      log.error(String.format("Failed to parse JSON for Feast job: %s", e.getMessage()));
    }

    return Optional.ofNullable(job);
  }

  @Override
  public void upsert(Job job) {
    try {
      jedisPool
          .getResource()
          .set(job.getId(), JsonFormat.printer().omittingInsignificantWhitespace().print(job));
    } catch (InvalidProtocolBufferException e) {
      log.error(String.format("Failed to upsert job: %s", e.getMessage()));
    }
  }
}
