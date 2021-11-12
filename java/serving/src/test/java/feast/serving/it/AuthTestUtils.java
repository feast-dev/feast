/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright 2018-2020 The Feast Authors
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
 */
package feast.serving.it;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.protobuf.Timestamp;
import feast.common.auth.credentials.OAuthCredentials;
import feast.proto.core.CoreServiceGrpc;
import feast.proto.serving.ServingAPIProto;
import feast.proto.serving.ServingAPIProto.GetOnlineFeaturesRequestV2;
import feast.proto.serving.ServingServiceGrpc;
import feast.proto.types.ValueProto.Value;
import io.grpc.CallCredentials;
import io.grpc.Channel;
import io.grpc.ManagedChannelBuilder;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import org.junit.runners.model.InitializationError;
import sh.ory.keto.ApiClient;
import sh.ory.keto.ApiException;
import sh.ory.keto.Configuration;
import sh.ory.keto.api.EnginesApi;
import sh.ory.keto.model.OryAccessControlPolicy;
import sh.ory.keto.model.OryAccessControlPolicyRole;

public class AuthTestUtils {

  private static final String DEFAULT_FLAVOR = "glob";

  public static GetOnlineFeaturesRequestV2 createOnlineFeatureRequest(
      String projectName,
      String featureTableName,
      String featureName,
      String entityId,
      int entityValue) {
    return GetOnlineFeaturesRequestV2.newBuilder()
        .setProject(projectName)
        .addFeatures(
            ServingAPIProto.FeatureReferenceV2.newBuilder()
                .setFeatureTable(featureTableName)
                .setName(featureName)
                .build())
        .addEntityRows(
            GetOnlineFeaturesRequestV2.EntityRow.newBuilder()
                .setTimestamp(Timestamp.newBuilder().setSeconds(100))
                .putFields(entityId, Value.newBuilder().setInt64Val(entityValue).build()))
        .build();
  }

  public static CoreSimpleAPIClient getSecureApiClientForCore(
      int feastCorePort, Map<String, String> options) {
    CallCredentials callCredentials = null;
    callCredentials = new OAuthCredentials(options);
    Channel secureChannel =
        ManagedChannelBuilder.forAddress("localhost", feastCorePort).usePlaintext().build();

    CoreServiceGrpc.CoreServiceBlockingStub secureCoreService =
        CoreServiceGrpc.newBlockingStub(secureChannel).withCallCredentials(callCredentials);

    return new CoreSimpleAPIClient(secureCoreService);
  }

  public static ServingServiceGrpc.ServingServiceBlockingStub getServingServiceStub(
      boolean isSecure, int feastServingPort, Map<String, String> options) {
    Channel secureChannel =
        ManagedChannelBuilder.forAddress("localhost", feastServingPort).usePlaintext().build();

    if (isSecure) {
      CallCredentials callCredentials = null;
      callCredentials = new OAuthCredentials(options);
      return ServingServiceGrpc.newBlockingStub(secureChannel).withCallCredentials(callCredentials);
    } else {
      return ServingServiceGrpc.newBlockingStub(secureChannel);
    }
  }

  public static void seedHydra(
      String hydraExternalUrl,
      String clientId,
      String clientSecrret,
      String audience,
      String grantType)
      throws IOException, InitializationError {

    OkHttpClient httpClient = new OkHttpClient();
    String createClientEndpoint = String.format("%s/%s", hydraExternalUrl, "clients");
    JsonObject jsonObject = new JsonObject();
    JsonArray audienceArrray = new JsonArray();
    audienceArrray.add(audience);
    JsonArray grantTypes = new JsonArray();
    grantTypes.add(grantType);
    jsonObject.addProperty("client_id", clientId);
    jsonObject.addProperty("client_secret", clientSecrret);
    jsonObject.addProperty("token_endpoint_auth_method", "client_secret_post");
    jsonObject.add("audience", audienceArrray);
    jsonObject.add("grant_types", grantTypes);
    MediaType JSON = MediaType.parse("application/json; charset=utf-8");

    RequestBody requestBody = RequestBody.create(JSON, jsonObject.toString());
    Request request =
        new Request.Builder()
            .url(createClientEndpoint)
            .addHeader("Content-Type", "application/json")
            .post(requestBody)
            .build();
    Response response = httpClient.newCall(request).execute();
    if (!response.isSuccessful()) {
      throw new InitializationError(response.message());
    }
  }

  public static void seedKeto(String url, String project, String subjectInProject, String admin)
      throws ApiException {
    ApiClient ketoClient = Configuration.getDefaultApiClient();
    ketoClient.setBasePath(url);
    EnginesApi enginesApi = new EnginesApi(ketoClient);

    // Add policies
    OryAccessControlPolicy adminPolicy = getAdminPolicy();
    enginesApi.upsertOryAccessControlPolicy(DEFAULT_FLAVOR, adminPolicy);

    OryAccessControlPolicy projectPolicy = getMyProjectMemberPolicy(project);
    enginesApi.upsertOryAccessControlPolicy(DEFAULT_FLAVOR, projectPolicy);

    // Add policy roles
    OryAccessControlPolicyRole adminPolicyRole = getAdminPolicyRole(admin);
    enginesApi.upsertOryAccessControlPolicyRole(DEFAULT_FLAVOR, adminPolicyRole);

    OryAccessControlPolicyRole myProjectMemberPolicyRole =
        getMyProjectMemberPolicyRole(project, subjectInProject);
    enginesApi.upsertOryAccessControlPolicyRole(DEFAULT_FLAVOR, myProjectMemberPolicyRole);
  }

  private static OryAccessControlPolicyRole getMyProjectMemberPolicyRole(
      String project, String subjectInProject) {
    OryAccessControlPolicyRole role = new OryAccessControlPolicyRole();
    role.setId(String.format("roles:%s-project-members", project));
    role.setMembers(Collections.singletonList("users:" + subjectInProject));
    return role;
  }

  private static OryAccessControlPolicyRole getAdminPolicyRole(String subjectIsAdmin) {
    OryAccessControlPolicyRole role = new OryAccessControlPolicyRole();
    role.setId("roles:admin");
    role.setMembers(Collections.singletonList("users:" + subjectIsAdmin));
    return role;
  }

  private static OryAccessControlPolicy getAdminPolicy() {
    OryAccessControlPolicy policy = new OryAccessControlPolicy();
    policy.setId("policies:admin");
    policy.subjects(Collections.singletonList("roles:admin"));
    policy.resources(Collections.singletonList("resources:**"));
    policy.actions(Collections.singletonList("actions:**"));
    policy.effect("allow");
    policy.conditions(null);
    return policy;
  }

  private static OryAccessControlPolicy getMyProjectMemberPolicy(String project) {
    OryAccessControlPolicy policy = new OryAccessControlPolicy();
    policy.setId(String.format("policies:%s-project-members-policy", project));
    policy.subjects(Collections.singletonList(String.format("roles:%s-project-members", project)));
    policy.resources(
        Arrays.asList(
            String.format("resources:projects:%s", project),
            String.format("resources:projects:%s:**", project)));
    policy.actions(Collections.singletonList("actions:**"));
    policy.effect("allow");
    policy.conditions(null);
    return policy;
  }
}
