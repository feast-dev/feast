package feast.serving;

import feast.serving.service.serving.ServingService;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
public class ServingApiApplicationTest {

  // @TestConfiguration
  // static class ServingApiApplicationTestContextConfiguration {
  //
  //   @Bean
  //   public FeastServing feastServing() {
  //     return null;
  //   }
  // }

  @MockBean
  private ServingService servingService;

  @Before
  public void setUp() {

  }

  @Test
  public void test() {

  }
}
