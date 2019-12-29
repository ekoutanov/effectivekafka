package effectivekafka.layeredconsumer;

import effectivekafka.layeredconsumer.event.*;

public final class CustomerBusinessLogic {
  public CustomerBusinessLogic(EventReceiver receiver) {
    receiver.addListener(this::onEvent);
  }
  
  private void onEvent(CustomerEvent event, ReceiveError error) {
    System.out.format("Received %s%n", event);
  }
}
