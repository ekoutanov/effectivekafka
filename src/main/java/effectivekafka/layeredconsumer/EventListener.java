package effectivekafka.layeredconsumer;

@FunctionalInterface
public interface EventListener {
  void onEvent(CustomerEvent event, ReceiveError error);
}
