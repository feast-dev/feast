package feast.core.job.flink;

import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertThat;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Arrays;
import okhttp3.HttpUrl;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.springframework.web.client.RestTemplate;

public class FlinkRestApiTest {
  FlinkRestApi flinkRestApi;
  MockWebServer mockWebServer;

  String host;
  int port;

  @Before
  public void setUp() throws Exception {
    mockWebServer = new MockWebServer();
    mockWebServer.start();

    port = mockWebServer.getPort();
    host = mockWebServer.getHostName();

    flinkRestApi = new FlinkRestApi(new RestTemplate(), String.format("%s:%d", host, port));
  }

  @Test
  public void shouldSendCorrectRequest() throws InterruptedException {
    MockResponse response = new MockResponse();
    response.setResponseCode(200);
    mockWebServer.enqueue(response);

    flinkRestApi.getJobsOverview();

    RecordedRequest recordedRequest = mockWebServer.takeRequest();
    HttpUrl requestUrl = recordedRequest.getRequestUrl();

    assertThat(requestUrl.host(), equalTo(host));
    assertThat(requestUrl.port(), equalTo(port));
    assertThat(requestUrl.encodedPath(), equalTo("/jobs/overview"));
  }

  @Test
  public void shouldReturnEmptyJobListForEmptyBody() {
    MockResponse response = new MockResponse();
    response.setResponseCode(200);
    mockWebServer.enqueue(response);

    FlinkJobList jobList = flinkRestApi.getJobsOverview();
    assertThat(jobList.getJobs().size(), equalTo(0));
  }

  @Test
  public void shouldReturnEmptyJobListForEmptyJsonResponse() {
    mockWebServer.enqueue(createMockResponse(200, "[]"));

    FlinkJobList jobList = flinkRestApi.getJobsOverview();
    assertThat(jobList.getJobs().size(), equalTo(0));

    mockWebServer.enqueue(createMockResponse(200, "{}"));

    jobList = flinkRestApi.getJobsOverview();
    assertThat(jobList.getJobs().size(), equalTo(0));

    mockWebServer.enqueue(createMockResponse(200, "{jobs: []}"));

    jobList = flinkRestApi.getJobsOverview();
    assertThat(jobList.getJobs().size(), equalTo(0));
  }

  @Test
  public void shouldReturnCorrectResultForValidResponse() throws JsonProcessingException {
    FlinkJobList jobList = new FlinkJobList();
    FlinkJob job1 = new FlinkJob("1234", "job1", "RUNNING");
    FlinkJob job2 = new FlinkJob("5678", "job2", "RUNNING");
    FlinkJob job3 = new FlinkJob("1111", "job3", "RUNNING");

    jobList.setJobs(Arrays.asList(job1, job2, job3));

    mockWebServer.enqueue( createMockResponse(200, createResponseBody(jobList)));

    FlinkJobList actual = flinkRestApi.getJobsOverview();

    assertThat(actual.getJobs().size(), equalTo(3));
  }

  @After
  public void tearDown() throws Exception {
    mockWebServer.shutdown();
  }

  private String createResponseBody(FlinkJobList jobList) throws JsonProcessingException {
    ObjectMapper objectMapper = new ObjectMapper();
    return objectMapper.writeValueAsString(jobList);
  }

  private MockResponse createMockResponse(int statusCode, String body) {
    MockResponse response = new MockResponse();
    response.setHeader("Content-Type", "application/json");
    response.setResponseCode(statusCode);
    response.setBody(body);
    return  response;
  }
}
