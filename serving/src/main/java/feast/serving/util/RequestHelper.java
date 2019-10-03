package feast.serving.util;

import feast.serving.ServingAPIProto.GetFeaturesRequest;
import io.grpc.Status;

public class RequestHelper {

  public static void validateRequest(GetFeaturesRequest request) {
    // EntityDataSetRow shall not be empty
    if (request.getEntityRowsCount() <= 0) {
      throw Status.INVALID_ARGUMENT.withDescription("Entity value must be provided")
          .asRuntimeException();
    }
  }
}
