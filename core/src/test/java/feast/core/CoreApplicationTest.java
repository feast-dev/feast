/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright 2018-2019 The Feast Authors
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
package feast.core;
//
// import static feast.core.config.StorageConfig.DEFAULT_SERVING_ID;
// import static feast.core.config.StorageConfig.DEFAULT_WAREHOUSE_ID;
// import static org.junit.Assert.assertEquals;
// import static org.mockito.ArgumentMatchers.any;
// import static org.mockito.Mockito.when;
//
// import feast.core.config.ImportJobDefaults;
// import feast.core.job.JobManager;
// import feast.core.model.StorageInfo;
// import feast.core.service.SpecService;
// import feast.core.stream.FeatureStream;
// import feast.specs.EntitySpecProto.EntitySpec;
// import feast.specs.FeatureSpecProto.FeatureSpec;
// import feast.specs.StorageSpecProto.StorageSpec;
// import feast.types.ValueProto.ValueType;
// import io.grpc.ManagedChannel;
// import io.grpc.ManagedChannelBuilder;
// import java.io.IOException;
// import java.nio.file.Files;
// import java.nio.file.Paths;
// import java.util.Collections;
// import java.util.HashMap;
// import java.util.List;
// import java.util.Map;
// import org.junit.Test;
// import org.junit.runner.RunWith;
// import org.mockito.ArgumentMatchers;
// import org.mockito.Mockito;
// import org.mockito.stubbing.Answer;
// import org.springframework.beans.factory.annotation.Autowired;
// import org.springframework.boot.test.context.SpringBootTest;
// import org.springframework.boot.test.context.TestConfiguration;
// import org.springframework.context.annotation.Bean;
// import org.springframework.test.annotation.DirtiesContext;
// import org.springframework.test.context.junit4.SpringRunner;
//
/// **
// * Starts the application context with some properties
// */
// @RunWith(SpringRunner.class)
// @SpringBootTest(properties = {
//    "feast.jobs.workspace=${java.io.tmpdir}/${random.uuid}",
//    "spring.datasource.url=jdbc:h2:mem:testdb",
//    "feast.store.warehouse.type=FILE.JSON",
//    "feast.store.warehouse.options={\"path\":\"/tmp/foobar\"}",
//    "feast.store.serving.type=REDIS",
//    "feast.store.serving.options={\"host\":\"localhost\",\"port\":1234}",
//    "feast.store.errors.type=STDERR",
//    "feast.stream.type=kafka",
//    "feast.stream.options={\"bootstrapServers\":\"localhost:8081\"}"
// })
// @DirtiesContext
public class CoreApplicationTest {
  //
  //  @Autowired
  //  SpecService specService;
  //  @Autowired
  //  ImportJobDefaults jobDefaults;
  //  @Autowired
  //  JobManager jobManager;
  //  @Autowired
  //  FeatureStream featureStream;
  //
  //  @Test
  //  public void test_withProperties_systemServingAndWarehouseStoresRegistered() throws IOException
  // {
  //    Files.createDirectory(Paths.get(jobDefaults.getWorkspace()));
  //
  //    List<StorageInfo> warehouseStorageInfo = specService
  //        .getStorage(Collections.singletonList(DEFAULT_WAREHOUSE_ID));
  //    assertEquals(warehouseStorageInfo.size(), 1);
  //    assertEquals(warehouseStorageInfo.get(0).getStorageSpec(), StorageSpec.newBuilder()
  //        .setId(DEFAULT_WAREHOUSE_ID).setType("FILE.JSON").putOptions("path", "/tmp/foobar")
  //        .build());
  //
  //    List<StorageInfo> servingStorageInfo = specService
  //        .getStorage(Collections.singletonList(DEFAULT_SERVING_ID));
  //    assertEquals(servingStorageInfo.size(), 1);
  //    assertEquals(servingStorageInfo.get(0).getStorageSpec(), StorageSpec.newBuilder()
  //        .setId(DEFAULT_SERVING_ID).setType("REDIS")
  //        .putOptions("host", "localhost")
  //        .putOptions("port", "1234")
  //        .build());
  //
  //    ManagedChannelBuilder<?> channelBuilder = ManagedChannelBuilder.forAddress("localhost",
  // 6565);
  //    ManagedChannel channel = channelBuilder.usePlaintext(true).build();
  //    CoreServiceGrpc.CoreServiceBlockingStub coreService =
  // CoreServiceGrpc.newBlockingStub(channel);
  //
  //    EntitySpec entitySpec = EntitySpec.newBuilder().setName("test").build();
  //    FeatureSpec featureSpec = FeatureSpec.newBuilder()
  //        .setId("test.int64")
  //        .setName("int64")
  //        .setEntity("test")
  //        .setValueType(ValueType.Enum.INT64)
  //        .setOwner("hermione@example.com")
  //        .setDescription("Test is a test")
  //        .setUri("http://example.com/test.int64").build();
  //
  //    when(featureStream.generateTopicName(ArgumentMatchers.anyString())).thenReturn("my-topic");
  //    when(featureStream.getType()).thenReturn("kafka");
  //
  //    coreService.applyEntity(entitySpec);
  //
  //    Map<Integer, Object> args = new HashMap<>();
  //    when(jobManager.startJob(any(), any())).thenAnswer((Answer<String>) invocation -> {
  //      args.put(0, invocation.getArgument(0));
  //      args.put(1, invocation.getArgument(1));
  //      return "externalJobId1234";
  //    });
  //
  //    coreService.applyFeature(featureSpec);
  //  }
  //
  //  @TestConfiguration
  //  public static class MockProvider {
  //
  //    @Bean
  //    public JobManager jobManager() {
  //      return Mockito.mock(JobManager.class);
  //    }
  //
  //    @Bean
  //    public FeatureStream featureStream() {
  //      return Mockito.mock(FeatureStream.class);
  //    }
  //  }
}
