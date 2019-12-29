package effectivekafka.layeredconsumer;

import effectivekafka.layeredconsumer.event.*;

@FunctionalInterface
public interface EventListener {
  void onEvent(CustomerEvent event, ReceiveError error);
}
