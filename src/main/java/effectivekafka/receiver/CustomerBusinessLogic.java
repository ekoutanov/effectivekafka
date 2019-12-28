package effectivekafka.receiver;

import java.io.*;

public final class CustomerBusinessLogic implements Closeable {
  private final EventReceiver receiver;
  
  public CustomerBusinessLogic(EventReceiver receiver) {
    this.receiver = receiver;
    receiver.addListener(this::onEvent);
  }
  
  private void onEvent(CustomerEvent event, ReceiveError error) {
    // do something with event 
  }
  
  @Override
  public void close() {
    receiver.close();
  }
}
