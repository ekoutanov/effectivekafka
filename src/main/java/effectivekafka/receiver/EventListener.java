package effectivekafka.receiver;

@FunctionalInterface
public interface EventListener {
  void onEvent(CustomerEvent event, ReceiveError error);
}
