package feast.serving.service;

import feast.serving.ServingAPIProto.Job;
import java.util.Optional;

// No-op implementation of the JobService, for online serving stores.
public class NoopJobService implements JobService {

  @Override
  public Optional<Job> get(String id) {
    return Optional.empty();
  }

  @Override
  public void upsert(Job job) {}
}
