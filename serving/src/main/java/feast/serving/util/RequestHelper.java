package feast.serving.util;

import feast.serving.ServingAPIProto.GetBatchFeaturesRequest;
import feast.serving.ServingAPIProto.GetOnlineFeaturesRequest;
import io.grpc.Status;

public class RequestHelper {

  public static void validateOnlineRequest(GetOnlineFeaturesRequest request) {
    // EntityDataSetRow shall not be empty
    if (request.getEntityRowsCount() <= 0) {
      throw Status.INVALID_ARGUMENT
          .withDescription("Entity value must be provided")
          .asRuntimeException();
    }
  }

  public static void validateBatchRequest(GetBatchFeaturesRequest getFeaturesRequest) {
    if (!getFeaturesRequest.hasDatasetSource()) {
      throw Status.INVALID_ARGUMENT
          .withDescription("Dataset source must be provided")
          .asRuntimeException();
    }

    if (!getFeaturesRequest.getDatasetSource().hasFileSource()) {
      throw Status.INVALID_ARGUMENT
          .withDescription("Dataset source must be provided: only file source supported")
          .asRuntimeException();
    }
  }
}
