package feast.serving.service;

import com.google.protobuf.InvalidProtocolBufferException;
import feast.serving.ServingAPIProto.FeastServingType;
import feast.serving.ServingAPIProto.GetBatchFeaturesFromCompletedJobRequest;
import feast.serving.ServingAPIProto.GetBatchFeaturesFromCompletedJobResponse;
import feast.serving.ServingAPIProto.GetBatchFeaturesResponse;
import feast.serving.ServingAPIProto.GetFeastServingTypeRequest;
import feast.serving.ServingAPIProto.GetFeastServingTypeResponse;
import feast.serving.ServingAPIProto.GetFeaturesRequest;
import feast.serving.ServingAPIProto.GetFeaturesRequest.EntityDatasetRow;
import feast.serving.ServingAPIProto.GetFeaturesRequest.FeatureSet;
import feast.serving.ServingAPIProto.GetOnlineFeaturesResponse;
import feast.serving.ServingAPIProto.GetOnlineFeaturesResponse.FeatureDataset;
import feast.serving.ServingAPIProto.GetStagingLocationRequest;
import feast.serving.ServingAPIProto.GetStagingLocationResponse;
import feast.serving.ServingAPIProto.LoadBatchFeaturesRequest;
import feast.serving.ServingAPIProto.LoadBatchFeaturesResponse;
import feast.serving.ServingAPIProto.ReloadJobStatusRequest;
import feast.serving.ServingAPIProto.ReloadJobStatusResponse;
import feast.storage.RedisProto.RedisKey;
import feast.types.FeatureRowProto.FeatureRow;
import feast.types.FieldProto;
import feast.types.FieldProto.Field;
import io.grpc.Status;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Pipeline;

public class RedisServingService implements ServingService {
  private final JedisPool jedisPool;

  public RedisServingService(String redisHost, int redisPort) {
    this.jedisPool = new JedisPool(redisHost, redisPort);
  }

  @Override
  public GetFeastServingTypeResponse getFeastServingType(
      GetFeastServingTypeRequest getFeastServingTypeRequest) {
    return GetFeastServingTypeResponse.newBuilder()
        .setType(FeastServingType.FEAST_SERVING_TYPE_ONLINE)
        .build();
  }

  @Override
  public GetOnlineFeaturesResponse getOnlineFeatures(GetFeaturesRequest getFeaturesRequest) {
    List<FeatureSet> featureSets = getFeaturesRequest.getFeatureSetsList();

    if (featureSets.size() == 0) {
      return GetOnlineFeaturesResponse.getDefaultInstance();
    }

    if (featureSets.size() > 1) {
      throw Status.INVALID_ARGUMENT
          .withDescription("GetOnlineFeatures only support 1 FeatureSet for now")
          .asRuntimeException();
    }

    FeatureSet featureSet = featureSets.get(0);
    List<String> entityNames = getFeaturesRequest.getEntityDataset().getEntityNamesList();
    List<EntityDatasetRow> entityDatasetRows =
        getFeaturesRequest.getEntityDataset().getEntityDatasetRowsList();
    Pipeline pipeline = jedisPool.getResource().pipelined();

    for (EntityDatasetRow entityDatasetRow : entityDatasetRows) {
      if (entityNames.size() != entityDatasetRow.getEntityIdsCount()) {
        // Redis key requires that all entity names have all the ids, otherwise there will be no
        // entry in the Redis. So we can skip retrieval for this case.
        continue;
      }

      String redisKeyFeatureSet = featureSet.getName() + ":" + featureSet.getVersion();
      List<Field> redisKeyEntities = new ArrayList<>();
      for (int i = 0; i < entityNames.size(); i++) {
        redisKeyEntities.add(
            FieldProto.Field.newBuilder()
                .setName(entityNames.get(i))
                .setValue(entityDatasetRow.getEntityIds(i))
                .build());
      }
      RedisKey redisKey =
          RedisKey.newBuilder()
              .setFeatureSet(redisKeyFeatureSet)
              .addAllEntities(redisKeyEntities)
              .build();
      pipeline.get(redisKey.toByteArray());
    }

    List<FeatureRow> featureRows =
        pipeline.syncAndReturnAll().stream()
            .filter(Objects::nonNull)
            .map(
                result -> {
                  try {
                    return FeatureRow.parseFrom((byte[]) result);
                  } catch (InvalidProtocolBufferException e) {
                    e.printStackTrace();
                  }
                  return FeatureRow.getDefaultInstance();
                })
            .collect(Collectors.toList());

    FeatureDataset featureDataset =
        FeatureDataset.newBuilder()
            .setName(featureSet.getName())
            .setVersion(featureSet.getVersion())
            .addAllFeatureRows(featureRows)
            .build();

    return GetOnlineFeaturesResponse.newBuilder().addFeatureDatasets(featureDataset).build();
  }

  @Override
  public GetBatchFeaturesResponse getBatchFeatures(GetFeaturesRequest getFeaturesRequest) {
    throw Status.UNIMPLEMENTED.withDescription("Method not implemented").asRuntimeException();
  }

  @Override
  public GetBatchFeaturesFromCompletedJobResponse getBatchFeaturesFromCompletedJob(
      GetBatchFeaturesFromCompletedJobRequest getBatchFeaturesFromCompletedJobRequest) {
    throw Status.UNIMPLEMENTED.withDescription("Method not implemented").asRuntimeException();
  }

  @Override
  public GetStagingLocationResponse getStagingLocation(
      GetStagingLocationRequest getStagingLocationRequest) {
    throw Status.UNIMPLEMENTED.withDescription("Method not implemented").asRuntimeException();
  }

  @Override
  public LoadBatchFeaturesResponse loadBatchFeatures(
      LoadBatchFeaturesRequest loadBatchFeaturesRequest) {
    throw Status.UNIMPLEMENTED.withDescription("Method not implemented").asRuntimeException();
  }

  @Override
  public ReloadJobStatusResponse reloadJobStatus(ReloadJobStatusRequest reloadJobStatusRequest) {
    throw Status.UNIMPLEMENTED.withDescription("Method not implemented").asRuntimeException();
  }
}
