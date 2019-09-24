// package feast.serving.service.serving;
//
// import com.google.cloud.bigquery.BigQuery;
// import feast.serving.ServingAPIProto.BatchFeaturesJob.GetDownloadUrlRequest;
// import feast.serving.ServingAPIProto.BatchFeaturesJob.GetDownloadUrlResponse;
// import feast.serving.ServingAPIProto.BatchFeaturesJob.GetStatusRequest;
// import feast.serving.ServingAPIProto.BatchFeaturesJob.GetStatusResponse;
// import feast.serving.ServingAPIProto.BatchFeaturesJob.GetUploadUrlRequest;
// import feast.serving.ServingAPIProto.BatchFeaturesJob.GetUploadUrlResponse;
// import feast.serving.ServingAPIProto.BatchFeaturesJob.SetUploadCompleteRequest;
// import feast.serving.ServingAPIProto.BatchFeaturesJob.SetUploadCompleteResponse;
// import feast.serving.ServingAPIProto.FeastServingType;
// import feast.serving.ServingAPIProto.GetBatchFeaturesResponse;
// import feast.serving.ServingAPIProto.GetFeastServingTypeResponse;
// import feast.serving.ServingAPIProto.GetFeaturesRequest;
// import feast.serving.ServingAPIProto.GetOnlineFeaturesResponse;
//
// public class BigQueryServingService implements ServingService {
//   private BigQuery bigquery;
//
//   public BigQueryServingService(BigQuery bigquery) {
//     this.bigquery = bigquery;
//   }
//
//   @Override
//   public GetFeastServingTypeResponse getFeastServingType() {
//     return GetFeastServingTypeResponse.newBuilder()
//         .setType(FeastServingType.FEAST_SERVING_TYPE_OFFLINE)
//         .build();
//   }
//
//   @Override
//   public GetOnlineFeaturesResponse getOnlineFeatures(GetFeaturesRequest request) {
//     throw new UnsupportedOperationException(
//         "BigQueryFeastServing only supports offline features retrieval");
//   }
//
//   @Override
//   public GetBatchFeaturesResponse getBatchFeatures(GetFeaturesRequest request) {
//
//     return null;
//   }
//
//   @Override
//   public GetStatusResponse getBatchFeaturesJobStatus(GetStatusRequest request) {
//     return null;
//   }
//
//   @Override
//   public GetDownloadUrlResponse getBatchFeaturesDownloadUrl(GetDownloadUrlRequest request) {
//     return null;
//   }
//
//   @Override
//   public GetUploadUrlResponse getBatchFeaturesJobUploadUrl(GetUploadUrlRequest request) {
//     return null;
//   }
//
//   @Override
//   public SetUploadCompleteResponse setBatchFeaturesJobUploadComplete(
//       SetUploadCompleteRequest request) {
//     return null;
//   }
// }
