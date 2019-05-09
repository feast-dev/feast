/*
 * Copyright 2018 The Feast Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package feast.core.grpc;

import com.google.common.base.Strings;
import com.google.protobuf.Timestamp;
import feast.core.DatasetServiceGrpc.DatasetServiceImplBase;
import feast.core.DatasetServiceProto.DatasetInfo;
import feast.core.DatasetServiceProto.FeatureSet;
import feast.core.DatasetServiceProto.DatasetServiceTypes.CreateDatasetRequest;
import feast.core.DatasetServiceProto.DatasetServiceTypes.CreateDatasetResponse;
import feast.core.training.BigQueryTraningDatasetCreator;
import io.grpc.Status;
import io.grpc.Status.Code;
import io.grpc.stub.StreamObserver;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import lombok.extern.slf4j.Slf4j;
import org.lognet.springboot.grpc.GRpcService;
import org.springframework.beans.factory.annotation.Autowired;

@Slf4j
@GRpcService
public class DatasetServiceImpl extends DatasetServiceImplBase {

  private final BigQueryTraningDatasetCreator datasetCreator;

  @Autowired
  public DatasetServiceImpl(BigQueryTraningDatasetCreator DatasetCreator) {
    this.datasetCreator = DatasetCreator;
  }

  @Override
  public void createDataset(
      CreateDatasetRequest request,
      StreamObserver<CreateDatasetResponse> responseObserver) {
    try {
      checkRequest(request);
    } catch (IllegalArgumentException e) {
      responseObserver.onError(
          Status.fromCode(Code.INVALID_ARGUMENT)
              .withCause(e)
              .withDescription(e.getMessage())
              .asException());
      return;
    }

    try {
      DatasetInfo datasetInfo =
          datasetCreator.createDataset(
              request.getFeatureSet(),
              request.getStartDate(),
              request.getEndDate(),
              request.getLimit(),
              request.getNamePrefix());
      CreateDatasetResponse response =
          CreateDatasetResponse.newBuilder().setDatasetInfo(datasetInfo).build();

      responseObserver.onNext(response);
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error("Training dataset creation failed", e);
      responseObserver.onError(
          Status.fromCode(Code.INTERNAL)
              .withCause(e)
              .withDescription("Training dataset creation failed: " + e.getMessage())
              .asException());
    }
  }

  private void checkRequest(CreateDatasetRequest request) {
    FeatureSet featureSet = request.getFeatureSet();
    Timestamp startDate = request.getStartDate();
    Timestamp endDate = request.getEndDate();

    checkHasSameEntity(featureSet);
    checkStartIsBeforeEnd(startDate, endDate);
  }

  private void checkStartIsBeforeEnd(Timestamp startDate, Timestamp endDate) {
    Instant start = Instant.ofEpochSecond(startDate.getSeconds()).truncatedTo(ChronoUnit.DAYS);
    Instant end = Instant.ofEpochSecond(endDate.getSeconds()).truncatedTo(ChronoUnit.DAYS);

    if (start.compareTo(end) > 0) {
      throw new IllegalArgumentException("startDate is after endDate");
    }
  }

  private void checkHasSameEntity(FeatureSet featureSet) {
    String entityName = featureSet.getEntityName();
    if (Strings.isNullOrEmpty(entityName)) {
      throw new IllegalArgumentException("entity name in feature set is null or empty");
    }

    if (featureSet.getFeatureIdsCount() < 1) {
      throw new IllegalArgumentException("feature set is empty");
    }

    for (String featureId : featureSet.getFeatureIdsList()) {
      String entity = featureId.split("\\.")[0];
      if (!entityName.equals(entity)) {
        throw new IllegalArgumentException("feature set contains different entity name: " + entity);
      }
    }
  }
}
