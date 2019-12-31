package effectivekafka.customerevents.receiver;

public final class ConsumerBusinessLogic {
  public ConsumerBusinessLogic(EventReceiver receiver) {
    receiver.addListener(this::onEvent);
  }
  
  private void onEvent(ReceiveEvent event) {
    if (! event.isError()) {
      System.out.format("Received %s%n", event.getPayload());
    } else {
      System.err.format("Error in record %s: %s%n", event.getRecord(), event.getError());
    }
  }
}
