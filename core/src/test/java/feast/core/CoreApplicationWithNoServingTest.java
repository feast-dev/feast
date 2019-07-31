package feast.core;

import static feast.core.config.StorageConfig.DEFAULT_WAREHOUSE_ID;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.when;

import com.google.protobuf.Timestamp;
import feast.core.config.ImportJobDefaults;
import feast.core.job.JobManager;
import feast.core.model.StorageInfo;
import feast.core.service.FeatureStreamService;
import feast.core.service.SpecService;
import feast.core.stream.FeatureStream;
import feast.core.stream.kafka.KafkaFeatureStream;
import feast.specs.EntitySpecProto.EntitySpec;
import feast.specs.FeatureSpecProto.FeatureSpec;
import feast.specs.ImportSpecProto.Field;
import feast.specs.ImportSpecProto.ImportSpec;
import feast.specs.ImportSpecProto.Schema;
import feast.specs.StorageSpecProto.StorageSpec;
import feast.types.ValueProto.ValueType;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit4.SpringRunner;

/**
 * Starts the application context with some properties
 */
@RunWith(SpringRunner.class)
@SpringBootTest(properties = {
    "feast.jobs.workspace=${java.io.tmpdir}/${random.uuid}",
    "spring.datasource.url=jdbc:h2:mem:testdb",
    "feast.store.warehouse.type=file.json",
    "feast.store.warehouse.options={\"path\":\"/tmp/foobar\"}",
    "feast.store.errors.type=stderr",
    "feast.stream.type=kafka",
    "feast.stream.options={\"bootstrapServers\":\"localhost:8081\"}"
})
@DirtiesContext
public class CoreApplicationWithNoServingTest {

  @Autowired
  SpecService specService;
  @Autowired
  ImportJobDefaults jobDefaults;
  @Autowired
  JobManager jobManager;
  @Autowired
  FeatureStream featureStream;

  @Test
  public void test_withProperties_systemServingAndWarehouseStoresRegistered() throws IOException {
    Files.createDirectory(Paths.get(jobDefaults.getWorkspace()));

    List<StorageInfo> warehouseStorageInfo = specService
        .getStorage(Collections.singletonList(DEFAULT_WAREHOUSE_ID));
    assertEquals(warehouseStorageInfo.size(), 1);
    assertEquals(warehouseStorageInfo.get(0).getStorageSpec(), StorageSpec.newBuilder()
        .setId(DEFAULT_WAREHOUSE_ID).setType("file.json").putOptions("path", "/tmp/foobar")
        .build());

    ManagedChannelBuilder<?> channelBuilder = ManagedChannelBuilder.forAddress("localhost", 6565);
    ManagedChannel channel = channelBuilder.usePlaintext(true).build();
    CoreServiceGrpc.CoreServiceBlockingStub coreService = CoreServiceGrpc.newBlockingStub(channel);
    JobServiceGrpc.JobServiceBlockingStub jobService = JobServiceGrpc.newBlockingStub(channel);

    EntitySpec entitySpec = EntitySpec.newBuilder().setName("test").build();
    FeatureSpec featureSpec = FeatureSpec.newBuilder()
        .setId("test.int64")
        .setName("int64")
        .setEntity("test")
        .setValueType(ValueType.Enum.INT64)
        .setOwner("hermione@example.com")
        .setDescription("Test is a test")
        .setUri("http://example.com/test.int64").build();

    when(featureStream.generateTopicName(ArgumentMatchers.anyString())).thenReturn("my-topic");
    coreService.applyEntity(entitySpec);
    try {
      coreService.applyFeature(featureSpec);
      fail("should fail validation as there is not serving store");
    } catch (Exception e) {
    }
  }

  @TestConfiguration
  public static class MockProvider {

    @Bean
    public JobManager jobManager() {
      return Mockito.mock(JobManager.class);
    }

    @Bean
    public FeatureStream featureStream() {
      return Mockito.mock(FeatureStream.class);
    }
  }
}