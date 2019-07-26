package feast.core.stream.kafka;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import feast.core.exception.TopicExistsException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.common.KafkaFuture;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.ArgumentMatchers;
import org.mockito.Matchers;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class KafkaFeatureStreamTest {

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Mock
  private AdminClient kafkaAdminClient;
  @Mock
  private CreateTopicsResult createTopicsResult;

  private KafkaFeatureStream kafkaFeatureStream;

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
    KafkaFeatureStreamConfig config = new KafkaFeatureStreamConfig("localhost:8121", "feast", 1,
        (short) 2);
    kafkaFeatureStream = new KafkaFeatureStream(kafkaAdminClient, config);
  }

  @Test
  public void shouldThrowTopicExistsExceptionIfTopicExists()
      throws ExecutionException, InterruptedException {

    KafkaFuture<Void> result = mock(KafkaFuture.class);

    when(result.get()).thenThrow(new org.apache.kafka.common.errors.TopicExistsException(""));
    Map<String, KafkaFuture<Void>> resultMap = new HashMap<>();
    resultMap.put("my-topic", result);
    when(createTopicsResult.values()).thenReturn(resultMap);
    doReturn(createTopicsResult).when(kafkaAdminClient)
        .createTopics(ArgumentMatchers.anyCollection());

    expectedException.expect(TopicExistsException.class);
    kafkaFeatureStream.provisionTopic("my-topic");
  }
}

