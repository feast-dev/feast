package feast.serving.configuration;

import java.util.concurrent.ScheduledExecutorService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextClosedEvent;
import org.springframework.stereotype.Component;

@Component
public class ContextClosedHandler implements ApplicationListener<ContextClosedEvent> {

  @Autowired ScheduledExecutorService executor;

  @Override
  public void onApplicationEvent(ContextClosedEvent event) {
    executor.shutdown();
  }
}
