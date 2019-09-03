package feast.core.service;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

import com.google.common.collect.Lists;
import feast.core.dao.FeatureStreamTopicRepository;
import feast.core.exception.TopicExistsException;
import feast.core.model.FeatureSet;
import feast.core.model.FeatureStreamTopic;
import feast.core.stream.FeatureStream;
import java.util.Optional;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mock;

public class FeatureStreamServiceTest {

  @Mock
  private FeatureStream featureStream;

  @Mock
  private FeatureStreamTopicRepository featureStreamTopicRepository;

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  @Before
  public void setUp() {
    initMocks(this);
  }

  @Test
  public void shouldProvisionTopicGivenFeatureSet() {
    String topicName = "feast-featureSet-topic";
    FeatureSet featureSet = new FeatureSet();
    featureSet.setName("featureSet");
    when(featureStream.generateTopicName("featureSet")).thenReturn(topicName);

    FeatureStreamService featureStreamService = new FeatureStreamService(
        featureStreamTopicRepository, featureStream);
    FeatureStreamTopic actual = featureStreamService.provisionTopic(featureSet);

    FeatureStreamTopic expectedTopic = new FeatureStreamTopic(topicName,
        Lists.newArrayList(featureSet));
    verify(featureStream, times(1)).provisionTopic("feast-featureSet-topic");
    verify(featureStreamTopicRepository, times(1)).saveAndFlush(expectedTopic);
    assertThat(actual, equalTo(expectedTopic));
  }

  @Test
  public void shouldUpdateRecordIfSelfCreatedTopicExistsGivenFeatureSet() {
    String topicName = "feast-featureSet-topic";
    FeatureSet oldFeatureSet = new FeatureSet();
    oldFeatureSet.setName("featureSet");
    oldFeatureSet.setVersion(1);

    FeatureSet newFeatureSet = new FeatureSet();
    newFeatureSet.setName("featureSet");
    oldFeatureSet.setVersion(2);

    FeatureStreamTopic originalTopic = new FeatureStreamTopic(topicName,
        Lists.newArrayList(oldFeatureSet));

    when(featureStream.generateTopicName("featureSet")).thenReturn(topicName);
    doThrow(new TopicExistsException()).when(featureStream).provisionTopic(topicName);
    when(featureStreamTopicRepository.findById(topicName)).thenReturn(Optional.of(originalTopic));
    FeatureStreamService featureStreamService = new FeatureStreamService(
        featureStreamTopicRepository, featureStream);

    FeatureStreamTopic expectedTopic = new FeatureStreamTopic(topicName,
        Lists.newArrayList(oldFeatureSet, newFeatureSet));

    FeatureStreamTopic actual = featureStreamService.provisionTopic(newFeatureSet);
    verify(featureStreamTopicRepository, times(1)).saveAndFlush(expectedTopic);

    assertThat(actual, equalTo(expectedTopic));
  }

  @Test
  public void shouldThrowErrorIfTopicExistsGivenFeatureSet() {
    String topicName = "feast-featureSet-topic";

    FeatureSet featureSet = new FeatureSet();
    featureSet.setName("featureSet");

    when(featureStream.generateTopicName("featureSet")).thenReturn(topicName);
    doThrow(new TopicExistsException()).when(featureStream).provisionTopic(topicName);
    when(featureStreamTopicRepository.findById(topicName)).thenReturn(Optional.empty());

    FeatureStreamService featureStreamService = new FeatureStreamService(
        featureStreamTopicRepository, featureStream);

    expectedException.expect(TopicExistsException.class);
    featureStreamService.provisionTopic(featureSet);
  }


}