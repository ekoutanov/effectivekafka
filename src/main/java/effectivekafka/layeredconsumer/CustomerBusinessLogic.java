package effectivekafka.layeredconsumer;

public final class CustomerBusinessLogic {
  public CustomerBusinessLogic(EventReceiver receiver) {
    receiver.addListener(this::onEvent);
  }
  
  private void onEvent(ReceivedEvent event) {
    System.out.format("Received %s%n", event);
  }
}
