package feast.serving.service;

import feast.serving.ServingAPIProto.FeastServingType;
import feast.serving.ServingAPIProto.GetBatchFeaturesFromCompletedJobRequest;
import feast.serving.ServingAPIProto.GetBatchFeaturesFromCompletedJobResponse;
import feast.serving.ServingAPIProto.GetBatchFeaturesResponse;
import feast.serving.ServingAPIProto.GetFeastServingTypeRequest;
import feast.serving.ServingAPIProto.GetFeastServingTypeResponse;
import feast.serving.ServingAPIProto.GetFeaturesRequest;
import feast.serving.ServingAPIProto.GetOnlineFeaturesResponse;
import feast.serving.ServingAPIProto.GetStagingLocationRequest;
import feast.serving.ServingAPIProto.GetStagingLocationResponse;
import feast.serving.ServingAPIProto.LoadBatchFeaturesRequest;
import feast.serving.ServingAPIProto.LoadBatchFeaturesResponse;
import feast.serving.ServingAPIProto.ReloadJobStatusRequest;
import feast.serving.ServingAPIProto.ReloadJobStatusResponse;
import org.springframework.beans.factory.annotation.Value;
import redis.clients.jedis.JedisPool;

public class RedisServingService implements ServingService {

  @Value("${feast.version}")
  private String feastVersion;

  private final JedisPool jedisPool;

  public RedisServingService(String redisHost, int redisPort) {
    this.jedisPool = new JedisPool(redisHost, redisPort);
  }

  @Override
  public GetFeastServingTypeResponse GetFeastServingType(
      GetFeastServingTypeRequest getFeastServingTypeRequest) {
    return GetFeastServingTypeResponse.newBuilder()
        .setType(FeastServingType.FEAST_SERVING_TYPE_ONLINE)
        .build();
  }

  @Override
  public GetOnlineFeaturesResponse GetOnlineFeatures(GetFeaturesRequest getFeaturesRequest) {
    return null;
  }

  @Override
  public GetBatchFeaturesResponse GetBatchFeatures(GetFeaturesRequest getFeaturesRequest) {
    return null;
  }

  @Override
  public GetBatchFeaturesFromCompletedJobResponse GetBatchFeaturesFromCompletedJob(
      GetBatchFeaturesFromCompletedJobRequest getBatchFeaturesFromCompletedJobRequest) {
    return null;
  }

  @Override
  public GetStagingLocationResponse GetStagingLocation(
      GetStagingLocationRequest getStagingLocationRequest) {
    return null;
  }

  @Override
  public LoadBatchFeaturesResponse LoadBatchFeatures(
      LoadBatchFeaturesRequest loadBatchFeaturesRequest) {
    return null;
  }

  @Override
  public ReloadJobStatusResponse ReloadJobStatus(ReloadJobStatusRequest reloadJobStatusRequest) {
    return null;
  }
}
