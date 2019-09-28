package feast.serving.util;

import feast.serving.ServingAPIProto.GetFeaturesRequest;
import feast.serving.ServingAPIProto.GetFeaturesRequest.EntityDatasetRow;
import io.grpc.Status;

public class RequestHelper {

  public static void validateRequest(GetFeaturesRequest request) {
    // EntityDataSetRow shall not be empty
    if (request.getEntityDataset().getEntityDatasetRowsCount() <= 0) {
      throw Status.INVALID_ARGUMENT.withDescription("Entity value must be provided")
          .asRuntimeException();
    }

    // Value list size in EntityDataSetRow shall be the same as the size of fieldNames
    // First entity value will always be timestamp in EntityDataSetRow
    int fieldNameCount = request.getEntityDataset().getEntityNamesCount();
    for (EntityDatasetRow row : request.getEntityDataset().getEntityDatasetRowsList()) {
      if (row.getEntityIdsCount() != fieldNameCount) {
        throw Status.INVALID_ARGUMENT
            .withDescription("Size mismatch between fieldNames and its values")
            .asRuntimeException();
      }
    }
  }
}
