package feast.core;

import static feast.core.config.StorageConfig.DEFAULT_SERVING_ID;
import static feast.core.config.StorageConfig.DEFAULT_WAREHOUSE_ID;
import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;

import com.google.protobuf.Timestamp;
import feast.core.JobServiceProto.JobServiceTypes.SubmitImportJobRequest;
import feast.core.JobServiceProto.JobServiceTypes.SubmitImportJobResponse;
import feast.core.config.ImportJobDefaults;
import feast.core.job.JobManager;
import feast.core.model.StorageInfo;
import feast.core.service.SpecService;
import feast.specs.EntitySpecProto.EntitySpec;
import feast.specs.FeatureSpecProto.FeatureSpec;
import feast.specs.ImportJobSpecsProto.ImportJobSpecs;
import feast.specs.ImportSpecProto.Field;
import feast.specs.ImportSpecProto.ImportSpec;
import feast.specs.ImportSpecProto.Schema;
import feast.specs.StorageSpecProto.StorageSpec;
import feast.types.GranularityProto.Granularity.Enum;
import feast.types.ValueProto.ValueType;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.context.junit4.SpringRunner;

/**
 * Starts the application context with some properties
 */
@RunWith(SpringRunner.class)
@SpringBootTest(properties = {
    "feast.jobs.workspace=${java.io.tmpdir}${random.uuid}",
    "spring.datasource.url=jdbc:h2:mem:testdb",
    "feast.store.warehouse.type=file.json",
    "feast.store.warehouse.options={\"path\":\"/tmp/foobar\"}",
    "feast.store.serving.type=redis",
    "feast.store.serving.options={\"host\":\"localhost\",\"port\":1234}"
})
public class CoreApplicationTest {

  @Autowired
  SpecService specService;
  @Autowired
  ImportJobDefaults jobDefaults;
  @Autowired
  JobManager jobManager;

  @Test
  public void test_withProperties_systemServingAndWarehouseStoresRegistered() throws IOException {
    Files.createDirectory(Paths.get(jobDefaults.getWorkspace()));

    List<StorageInfo> warehouseStorageInfo = specService
        .getStorage(Collections.singletonList(DEFAULT_WAREHOUSE_ID));
    assertEquals(warehouseStorageInfo.size(), 1);
    assertEquals(warehouseStorageInfo.get(0).getStorageSpec(), StorageSpec.newBuilder()
        .setId(DEFAULT_WAREHOUSE_ID).setType("file.json").putOptions("path", "/tmp/foobar")
        .build());

    List<StorageInfo> servingStorageInfo = specService
        .getStorage(Collections.singletonList(DEFAULT_SERVING_ID));
    assertEquals(warehouseStorageInfo.size(), 1);
    assertEquals(servingStorageInfo.get(0).getStorageSpec(), StorageSpec.newBuilder()
        .setId(DEFAULT_SERVING_ID).setType("redis")
        .putOptions("host", "localhost")
        .putOptions("port", "1234")
        .build());

    ManagedChannelBuilder<?> channelBuilder = ManagedChannelBuilder.forAddress("localhost", 6565);
    ManagedChannel channel = channelBuilder.usePlaintext(true).build();
    CoreServiceGrpc.CoreServiceBlockingStub coreService = CoreServiceGrpc.newBlockingStub(channel);
    JobServiceGrpc.JobServiceBlockingStub jobService = JobServiceGrpc.newBlockingStub(channel);

    coreService.applyEntity(
        EntitySpec.newBuilder().setName("test").build());

    coreService.applyFeature(
        FeatureSpec.newBuilder()
            .setId("test.none.int64")
            .setName("int64")
            .setEntity("test")
            .setGranularity(Enum.NONE)
            .setValueType(ValueType.Enum.INT64)
            .setOwner("hermione@example.com")
            .setDescription("Test is a test")
            .setUri("http://example.com/test.none.int64").build());

    ImportSpec importSpec = ImportSpec.newBuilder()
        .setSchema(Schema.newBuilder()
            .setEntityIdColumn("id")
            .setTimestampValue(Timestamp.getDefaultInstance())
            .addFields(Field.newBuilder().setName("id"))
            .addFields(Field.newBuilder().setName("a").setFeatureId("test.none.int64")))
        .addEntities("test")
        .setType("file.csv")
        .putSourceOptions("path", "/tmp/foobar").build();
    SubmitImportJobRequest jobSubmitReq = SubmitImportJobRequest.newBuilder()
        .setImportSpec(importSpec).build();

    Map<Integer, Object> args = new HashMap<>();
    Mockito.when(jobManager.submitJob(any(), any())).thenAnswer((Answer<String>) invocation -> {
      args.put(0, invocation.getArgument(0));
      args.put(1, invocation.getArgument(1));
      return "externalJobId1234";
    });
    SubmitImportJobResponse jobSubmitRes = jobService.submitJob(jobSubmitReq);
    String jobId = jobSubmitRes.getJobId();
    assertEquals(args.get(1), Paths.get(jobDefaults.getWorkspace()).resolve(jobId));
    assertEquals(ImportJobSpecs.newBuilder()
        .setImportSpec(importSpec)
        .build(), args.get(0));
  }

  @TestConfiguration
  public static class MockProvider {

    @Bean
    public JobManager jobManager() {
      return Mockito.mock(JobManager.class);
    }
  }
}