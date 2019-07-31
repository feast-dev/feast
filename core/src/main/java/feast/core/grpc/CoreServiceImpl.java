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

import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.protobuf.Empty;
import com.google.protobuf.Timestamp;
import com.timgroup.statsd.StatsDClient;
import feast.core.CoreServiceGrpc.CoreServiceImplBase;
import feast.core.CoreServiceProto;
import feast.core.CoreServiceProto.CoreServiceTypes.*;
import feast.core.CoreServiceProto.CoreServiceTypes.GetUploadUrlResponse.HttpMethod;
import feast.core.config.StorageConfig.StorageSpecs;
import feast.core.exception.RegistrationException;
import feast.core.exception.RetrievalException;
import feast.core.model.EntityInfo;
import feast.core.model.FeatureGroupInfo;
import feast.core.model.FeatureInfo;
import feast.core.service.JobCoordinatorService;
import feast.core.service.SpecService;
import feast.core.validators.SpecValidator;
import feast.specs.EntitySpecProto.EntitySpec;
import feast.specs.FeatureGroupSpecProto;
import feast.specs.FeatureSpecProto.FeatureSpec;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang3.RandomUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.NameValuePair;
import org.apache.http.client.utils.URLEncodedUtils;
import org.lognet.springboot.grpc.GRpcService;
import org.springframework.beans.factory.annotation.Autowired;

import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/** Implementation of the feast core GRPC service. */
@Slf4j
@GRpcService
public class CoreServiceImpl extends CoreServiceImplBase {

  private static final String UPLOAD_URL_DIR = "uploads";
  private static final int UPLOAD_URL_VALIDITY_IN_MINUTES = 5;
  private Storage storage = StorageOptions.getDefaultInstance().getService();

  @Autowired private SpecService specService;
  @Autowired private SpecValidator validator;
  @Autowired private StatsDClient statsDClient;
  @Autowired private JobCoordinatorService jobCoordinatorService;
  @Autowired private StorageSpecs storageSpecs;

  public static long getUploadUrlValidityInMinutes() {
    return UPLOAD_URL_VALIDITY_IN_MINUTES;
  }

  public void setStorage(Storage storage) {
    this.storage = storage;
  }

  /**
   * Gets specs for all entities requested in the request. If the retrieval of any one of them
   * fails, the whole request will fail, giving an internal error.
   */
  @Override
  public void getEntities(
      GetEntitiesRequest request, StreamObserver<GetEntitiesResponse> responseObserver) {
    long now = System.currentTimeMillis();
    statsDClient.increment("get_entities_request_count");
    try {
      List<EntitySpec> entitySpecs =
          specService.getEntities(request.getIdsList()).stream()
              .map(EntityInfo::getEntitySpec)
              .collect(Collectors.toList());
      GetEntitiesResponse response =
          GetEntitiesResponse.newBuilder().addAllEntities(entitySpecs).build();
      responseObserver.onNext(response);
      responseObserver.onCompleted();
      statsDClient.increment("get_entities_request_success");
    } catch (RetrievalException | IllegalArgumentException e) {
      statsDClient.increment("get_entities_request_failed");
      log.error("Error in getEntities: {}", e);
      responseObserver.onError(getRuntimeException(e));
    } finally {
      long duration = System.currentTimeMillis() - now;
      statsDClient.gauge("get_entities_latency_ms", duration);
    }
  }

  /** Gets specs for all entities registered in the registry. */
  @Override
  public void listEntities(Empty request, StreamObserver<ListEntitiesResponse> responseObserver) {
    long now = System.currentTimeMillis();
    statsDClient.increment("list_entities_request_count");
    try {
      List<EntitySpec> entitySpecs =
          specService.listEntities().stream()
              .map(EntityInfo::getEntitySpec)
              .collect(Collectors.toList());
      ListEntitiesResponse response =
          ListEntitiesResponse.newBuilder().addAllEntities(entitySpecs).build();
      responseObserver.onNext(response);
      responseObserver.onCompleted();
      statsDClient.increment("list_entities_request_success");
    } catch (RetrievalException e) {
      statsDClient.increment("list_entities_request_failed");
      log.error("Error in listEntities: {}", e);
      responseObserver.onError(getRuntimeException(e));
    } finally {
      long duration = System.currentTimeMillis() - now;
      statsDClient.gauge("list_entities_latency_ms", duration);
    }
  }

  /**
   * Gets specs for all features requested in the request. If the retrieval of any one of them
   * fails, the whole request will fail, giving an internal error.
   */
  @Override
  public void getFeatures(
      GetFeaturesRequest request, StreamObserver<GetFeaturesResponse> responseObserver) {
    long now = System.currentTimeMillis();
    statsDClient.increment("get_features_request_count");
    try {
      List<FeatureSpec> featureSpecs =
          specService.getFeatures(request.getIdsList()).stream()
              .map(FeatureInfo::getFeatureSpec)
              .collect(Collectors.toList());
      GetFeaturesResponse response =
          GetFeaturesResponse.newBuilder().addAllFeatures(featureSpecs).build();
      responseObserver.onNext(response);
      responseObserver.onCompleted();
      statsDClient.increment("get_features_request_success");
    } catch (RetrievalException | IllegalArgumentException e) {
      statsDClient.increment("get_features_request_failed");
      log.error("Error in getFeatures: {}", e);
      responseObserver.onError(getRuntimeException(e));
    } finally {
      long duration = System.currentTimeMillis() - now;
      statsDClient.gauge("get_features_latency_ms", duration);
    }
  }

  /** Gets specs for all features registered in the registry. TODO: some kind of pagination */
  @Override
  public void listFeatures(Empty request, StreamObserver<ListFeaturesResponse> responseObserver) {
    long now = System.currentTimeMillis();
    statsDClient.increment("list_features_request_count");
    try {
      List<FeatureSpec> featureSpecs =
          specService.listFeatures().stream()
              .map(FeatureInfo::getFeatureSpec)
              .collect(Collectors.toList());
      ListFeaturesResponse response =
          ListFeaturesResponse.newBuilder().addAllFeatures(featureSpecs).build();
      responseObserver.onNext(response);
      responseObserver.onCompleted();
      statsDClient.increment("list_features_request_success");
    } catch (RetrievalException e) {
      statsDClient.increment("list_features_request_failed");
      log.error("Error in listFeatures: {}", e);
      responseObserver.onError(getRuntimeException(e));
    } finally {
      long duration = System.currentTimeMillis() - now;
      statsDClient.gauge("list_features_latency_ms", duration);
    }
  }

  /**
   * Registers a single feature spec to the registry. If validation fails, will returns a bad
   * request error. If registration fails (e.g. connection to the db is interrupted), an internal
   * error will be returned.
   */
  @Override
  public void applyFeature(
      FeatureSpec request, StreamObserver<ApplyFeatureResponse> responseObserver) {
    try {
      validator.validateFeatureSpec(request);
      FeatureInfo feature = specService.applyFeature(request);
      ApplyFeatureResponse response =
          ApplyFeatureResponse.newBuilder().setFeatureId(feature.getId()).build();
      responseObserver.onNext(response);
      responseObserver.onCompleted();
    } catch (RegistrationException e) {
      log.error("Error in applyFeature: {}", e);
      responseObserver.onError(getRuntimeException(e));
    } catch (IllegalArgumentException e) {
      log.error("Error in applyFeature: {}", e);
      responseObserver.onError(getBadRequestException(e));
    }
  }

  @Override
  public void applyFeatures(CoreServiceProto.CoreServiceTypes.ApplyFeaturesRequest request, StreamObserver<CoreServiceProto.CoreServiceTypes.ApplyFeaturesResponse> responseObserver) {
    ApplyFeaturesResponse applyFeaturesResponse = ApplyFeaturesResponse.newBuilder().build();
    responseObserver.onNext(applyFeaturesResponse);
    responseObserver.onCompleted();
  }

  /**
   * Registers a list of feature specs to the registry. If validation fails, will returns a bad
   * request error. If registration fails (e.g. connection to the db is interrupted), an internal
   * error will be returned.
   */
  @Override
  public void applyFeatures(ApplyFeaturesRequest request,
      StreamObserver<ApplyFeaturesResponse> responseObserver) {
    super.applyFeatures(request, responseObserver);
  }


  /**
   * Registers a single feature group spec to the registry. If validation fails, will returns a bad
   * request error. If registration fails (e.g. connection to the db is interrupted), an internal
   * error will be returned.
   */
  @Override
  public void applyFeatureGroup(
      FeatureGroupSpecProto.FeatureGroupSpec request,
      StreamObserver<ApplyFeatureGroupResponse> responseObserver) {
    try {
      validator.validateFeatureGroupSpec(request);
      FeatureGroupInfo featureGroup = specService.applyFeatureGroup(request);
      ApplyFeatureGroupResponse response =
          ApplyFeatureGroupResponse.newBuilder().setFeatureGroupId(featureGroup.getId()).build();
      responseObserver.onNext(response);
      responseObserver.onCompleted();
    } catch (RegistrationException e) {
      log.error("Error in applyFeatureGroup: {}", e);
      responseObserver.onError(getRuntimeException(e));
    } catch (IllegalArgumentException e) {
      log.error("Error in applyFeatureGroup: {}", e);
      responseObserver.onError(getBadRequestException(e));
    }
  }

  /**
   * Registers a single entity spec to the registry. If validation fails, will returns a bad request
   * error. If registration fails (e.g. connection to the db is interrupted), an internal error will
   * be returned.
   */
  @Override
  public void applyEntity(
      EntitySpec request, StreamObserver<ApplyEntityResponse> responseObserver) {
    try {
      validator.validateEntitySpec(request);
      EntityInfo entity = specService.applyEntity(request);
      ApplyEntityResponse response =
          ApplyEntityResponse.newBuilder().setEntityName(entity.getName()).build();
      responseObserver.onNext(response);
      responseObserver.onCompleted();
    } catch (RegistrationException e) {
      log.error("Error in applyEntity: {}", e);
      responseObserver.onError(getRuntimeException(e));
    } catch (IllegalArgumentException e) {
      log.error("Error in applyEntity: {}", e);
      responseObserver.onError(getBadRequestException(e));
    }
  }

  @Override
  public void getTopic(GetTopicRequest request, StreamObserver<GetTopicResponse> responseObserver) {
    GetTopicResponse response = GetTopicResponse.newBuilder().setMessageBrokerURI("localhost:9092").setTopicName("mytopic").build();
    responseObserver.onNext(response);
    responseObserver.onCompleted();
  }

  /**
   * Get a signed URL where a Feast client can upload a CSV or JSON file by making an HTTP PUT
   * request. The signed URL references a bucket and blob in Google Cloud Storage.
   *
   * @param request
   * @param responseObserver
   */
  @Override
  public void getUploadUrl(
      GetUploadUrlRequest request, StreamObserver<GetUploadUrlResponse> responseObserver) {
    String bucketName = null;

    try {
      bucketName = getBucketNameFromWorkspace(jobCoordinatorService.getWorkspace());
    } catch (IllegalArgumentException e) {
      responseObserver.onError(
          Status.FAILED_PRECONDITION
              .withDescription("Failed to get upload URL from workspace\n" + e.getMessage())
              .asRuntimeException());
    }
    assert StringUtils.isNotEmpty(bucketName);

    // Generated file names are always unique
    String fileName =
        String.format(
            "%s-%s", System.currentTimeMillis(), DigestUtils.sha1Hex(RandomUtils.nextBytes(8)));

    BlobInfo blobInfo =
        BlobInfo.newBuilder(
                bucketName,
                String.format(
                    "%s/%s.%s",
                    UPLOAD_URL_DIR, fileName, request.getFileType().toString().toLowerCase()))
            .build();

    URL signedUrl = null;
    try {
      signedUrl =
          storage.signUrl(
              blobInfo,
              UPLOAD_URL_VALIDITY_IN_MINUTES,
              TimeUnit.MINUTES,
              Storage.SignUrlOption.httpMethod(com.google.cloud.storage.HttpMethod.PUT));
    } catch (Exception e) {
      responseObserver.onError(
          Status.FAILED_PRECONDITION
              .withDescription(
                  "Failed to create signed URL. Please check your Feast deployment config\n"
                      + e.getMessage())
              .asRuntimeException());
    }
    assert signedUrl != null;
    long expiryInEpochTime = -1;

    // Retrieve the actual expiry timestamp from the created signed URL
    try {
      List<NameValuePair> params =
          URLEncodedUtils.parse(signedUrl.toURI(), Charset.forName("UTF-8"));
      for (NameValuePair param : params) {
        if (param.getName().equals("Expires")) {
          expiryInEpochTime = Long.parseLong(param.getValue());
        }
      }
    } catch (URISyntaxException e) {
      responseObserver.onError(
          Status.UNKNOWN
              .withDescription("Failed to parse signed upload URL\n" + e.getMessage())
              .asRuntimeException());
    }

    GetUploadUrlResponse response =
        GetUploadUrlResponse.newBuilder()
            .setUrl(signedUrl.toString())
            .setPath(signedUrl.getPath().substring(1))
            .setHttpMethod(HttpMethod.PUT)
            .setExpiration(Timestamp.newBuilder().setSeconds(expiryInEpochTime))
            .build();

    responseObserver.onNext(response);
    responseObserver.onCompleted();
  }

  /**
   * Get Google Cloud Storage (GCS) bucket name from job workspace value
   *
   * @param workspace job workspace in Feast
   * @return bucket name
   * @throws IllegalArgumentException if workspace is not a valid GCS URI e.g when workspace is set
   *     to a local path
   */
  static String getBucketNameFromWorkspace(String workspace) throws IllegalArgumentException {
    if (StringUtils.isEmpty(workspace)) {
      throw new IllegalArgumentException("Workspace cannot be empty");
    }

    int start = workspace.indexOf("gs://");
    if (start < 0 || workspace.trim().length() <= 5) {
      throw new IllegalArgumentException(
          String.format(
              "Cannot get bucket from workspace '%s' because it does not start with gs://[bucket_name]",
              workspace));
    }

    // Find the index where the "bucket name" string ends
    // start searching after the string "gs://" (length of 5)
    int end = workspace.indexOf("/", 5);
    if (end < 0) {
      return workspace.substring(5);
    } else {
      return workspace.substring(5, end);
    }
  }

  private StatusRuntimeException getRuntimeException(Exception e) {
    return new StatusRuntimeException(
        Status.fromCode(Status.Code.INTERNAL).withDescription(e.getMessage()).withCause(e));
  }

  private StatusRuntimeException getBadRequestException(Exception e) {
    return new StatusRuntimeException(
        Status.fromCode(Status.Code.OUT_OF_RANGE).withDescription(e.getMessage()).withCause(e));
  }
}
