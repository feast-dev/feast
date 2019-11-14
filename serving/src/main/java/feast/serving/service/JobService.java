package feast.serving.service;

import feast.serving.ServingAPIProto.Job;
import java.util.Optional;

// JobService interface specifies the operations to manage Job instances internally in Feast

public interface JobService {

  /**
   * Get Job by job id.
   *
   * @param id job id
   * @return feast.serving.ServingAPIProto.Job
   */
  Optional<Job> get(String id);

  /**
   * Update or create a job (if not exists)
   *
   * @param job feast.serving.ServingAPIProto.Job
   */
  void upsert(Job job);
}
