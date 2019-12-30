package effectivekafka.layeredconsumer;

@FunctionalInterface
public interface EventListener {
  void onEvent(ReceivedEvent event);
}
