package feast.core.grpc;

import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import feast.core.CoreServiceGrpc;
import feast.core.CoreServiceGrpc.CoreServiceBlockingStub;
import feast.core.CoreServiceProto.CoreServiceTypes.GetUploadUrlRequest;
import feast.core.CoreServiceProto.CoreServiceTypes.GetUploadUrlRequest.FileType;
import feast.core.CoreServiceProto.CoreServiceTypes.GetUploadUrlResponse;
import feast.core.CoreServiceProto.CoreServiceTypes.GetUploadUrlResponse.HttpMethod;
import feast.core.service.JobManagementService;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.testing.GrpcCleanupRule;
import org.apache.commons.lang3.StringUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.File;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.*;
import static org.junit.Assume.assumeTrue;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class CoreServiceImplTest {
  @Rule public final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();

  @InjectMocks private CoreServiceImpl coreService = new CoreServiceImpl();

  @Mock private JobManagementService jobManagementService;

  @Test
  public void getUploadUrl() throws IOException {
    // Skip this test if GOOGLE_APPLICATION_CREDENTIALS is not set
    // because this test requires Google service account key and connection to "actual"
    // Google Cloud Storage service
    assumeTrue(StringUtils.isNotEmpty(System.getenv("GOOGLE_APPLICATION_CREDENTIALS")));

    // Mock the workspace output from jobManagementService to a bucket we can use for testing
    String bucketName = "feast-templocation-kf-feast";
    when(jobManagementService.getWorkspace()).thenReturn("gs://" + bucketName);

    // Start a local grpc server
    String serverName = InProcessServerBuilder.generateName();
    grpcCleanup.register(
        InProcessServerBuilder.forName(serverName)
            .directExecutor()
            .addService(coreService)
            .build()
            .start());
    CoreServiceBlockingStub blockingStub =
        CoreServiceGrpc.newBlockingStub(
            grpcCleanup.register(
                InProcessChannelBuilder.forName(serverName).directExecutor().build()));

    GetUploadUrlResponse response =
        blockingStub.getUploadUrl(
            GetUploadUrlRequest.newBuilder().setFileType(FileType.CSV).build());

    // Make sure the signed URL response allows upload using PUT method and the link is valid for
    // the next 5 minutes
    assertEquals(response.getHttpMethod(), HttpMethod.PUT);
    assertTrue(response.getExpiration().getSeconds() > System.currentTimeMillis() / 1000);
    assertTrue(
        response.getExpiration().getSeconds() < (System.currentTimeMillis() + 6 * 60_000) / 1000);

    // Test that we can upload data using the signed URL from the response
    String uploadUrl = response.getUrl();
    URL url = new URL(uploadUrl);
    HttpURLConnection conn = (HttpURLConnection) url.openConnection();
    conn.setDoOutput(true);
    conn.setRequestMethod("PUT");
    OutputStreamWriter out = new OutputStreamWriter(conn.getOutputStream());
    out.write("1,2,3\n4,5,6");
    out.close();
    conn.getInputStream();

    // Ensure the data is uploaded correctly
    Storage storage = StorageOptions.getDefaultInstance().getService();
    String fileName =
        uploadUrl.substring(
            "https://storage.googleapis.com/feast-templocation-kf-feast/".length(),
            uploadUrl.indexOf("?"));
    File tempFile = File.createTempFile("prefix", "suffix");
    tempFile.deleteOnExit();
    storage.get(BlobId.of(bucketName, fileName)).downloadTo(tempFile.toPath());
    List<String> strings = Files.readAllLines(tempFile.toPath());
    assertEquals(strings.get(0), "1,2,3");
    assertEquals(strings.get(1), "4,5,6");
  }

  @Test
  public void getBucketNameFromWorkspace() {
    List<Object[]> testCases =
        Arrays.asList(
            new Object[] {"gs://bucket", "bucket"},
            new Object[] {"gs://bucket/", "bucket"},
            new Object[] {"gs://bucket/dir", "bucket"},
            new Object[] {null, new IllegalArgumentException()},
            new Object[] {"", new IllegalArgumentException()},
            new Object[] {"/", new IllegalArgumentException()},
            new Object[] {"/local/dir", new IllegalArgumentException()},
            new Object[] {"gs", new IllegalArgumentException()},
            new Object[] {"gs", new IllegalArgumentException()},
            new Object[] {"gs://", new IllegalArgumentException()});

    for (Object[] testCase : testCases) {
      String input = (String) testCase[0];
      Object expected = testCase[1];

      try {
        assertEquals(expected, CoreServiceImpl.getBucketNameFromWorkspace(input));
      } catch (IllegalArgumentException e) {
        if (!(expected instanceof IllegalArgumentException)) {
          fail(
              String.format(
                  "Exception thrown for input '%s', expected output: '%s'", input, expected));
        }
      }
    }
  }
}
