package com.gojek.feast.v1alpha1;

import feast.serving.ServingAPIProto.GetFeastServingInfoRequest;
import feast.serving.ServingAPIProto.GetFeastServingInfoResponse;
import feast.serving.ServingAPIProto.GetOnlineFeaturesRequest;
import feast.serving.ServingAPIProto.GetOnlineFeaturesRequest.EntityRow;
import feast.serving.ServingAPIProto.GetOnlineFeaturesRequest.FeatureSet;
import feast.serving.ServingAPIProto.GetOnlineFeaturesResponse;
import feast.serving.ServingServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("WeakerAccess")
public class FeastClient implements AutoCloseable {
  Logger logger = LoggerFactory.getLogger(FeastClient.class);

  private static final int CHANNEL_SHUTDOWN_TIMEOUT_SEC = 5;

  private final ManagedChannel channel;
  private final ServingServiceGrpc.ServingServiceBlockingStub stub;

  /**
   * Create a client to access Feast
   *
   * @param host hostname or ip address of Feast serving GRPC server
   * @param port port number of Feast serving GRPC server
   * @return {@link FeastClient}
   */
  public static FeastClient create(String host, int port) {
    ManagedChannel channel = ManagedChannelBuilder.forAddress(host, port).usePlaintext().build();
    return new FeastClient(channel);
  }

  public GetFeastServingInfoResponse getFeastServingInfo() {
    return stub.getFeastServingInfo(GetFeastServingInfoRequest.newBuilder().build());
  }

  /**
   * Get online features from Feast.
   *
   * <p>See {@link #getOnlineFeatures(List, List, boolean)}
   *
   * @param featureIds list of feature id to retrieve, feature id follows this format
   *     [feature_set_name]:[version]:[feature_name]
   * @param rows list of {@link Row} to select the entities to retrieve the features for
   * @return list of {@link Row} containing features
   */
  public List<Row> getOnlineFeatures(List<String> featureIds, List<Row> rows) {
    return getOnlineFeatures(featureIds, rows, false);
  }

  /**
   * Get online features from Feast.
   *
   * <p>Example of retrieving online features for driver feature set, version 1, with features
   * driver_id and driver_name
   *
   * <pre>{@code
   * FeastClient client = FeastClient.create("localhost", 6566);
   * List<String> requestedFeatureIds = Arrays.asList("driver:1:driver_id", "driver:1:driver_name");
   * List<Row> requestedRows =
   *         Arrays.asList(Row.create().set("driver_id", 123), Row.create().set("driver_id", 456));
   * List<Row> retrievedFeatures = client.getOnlineFeatures(requestedFeatureIds, requestedRows);
   * retrievedFeatures.forEach(System.out::println);
   * }</pre>
   *
   * @param featureIds list of feature id to retrieve, feature id follows this format
   *     [feature_set_name]:[version]:[feature_name]
   * @param rows list of {@link Row} to select the entities to retrieve the features for
   * @param omitEntitiesInResponse if true, the returned {@link Row} will not contain field and
   *     value for the entity
   * @return list of {@link Row} containing features
   */
  public List<Row> getOnlineFeatures(
      List<String> featureIds, List<Row> rows, boolean omitEntitiesInResponse) {
    List<FeatureSet> featureSets = RequestUtil.createFeatureSets(featureIds);
    List<EntityRow> entityRows =
        rows.stream()
            .map(
                row ->
                    EntityRow.newBuilder()
                        .setEntityTimestamp(row.getEntityTimestamp())
                        .putAllFields(row.getFields())
                        .build())
            .collect(Collectors.toList());

    GetOnlineFeaturesResponse response =
        stub.getOnlineFeatures(
            GetOnlineFeaturesRequest.newBuilder()
                .addAllFeatureSets(featureSets)
                .addAllEntityRows(entityRows)
                .setOmitEntitiesInResponse(omitEntitiesInResponse)
                .build());

    return response.getFieldValuesList().stream()
        .map(
            field -> {
              Row row = Row.create();
              field.getFieldsMap().forEach(row::set);
              return row;
            })
        .collect(Collectors.toList());
  }

  private FeastClient(ManagedChannel channel) {
    this.channel = channel;
    stub = ServingServiceGrpc.newBlockingStub(channel);
  }

  public void close() throws Exception {
    if (channel != null) {
      channel.shutdown().awaitTermination(CHANNEL_SHUTDOWN_TIMEOUT_SEC, TimeUnit.SECONDS);
    }
  }
}
