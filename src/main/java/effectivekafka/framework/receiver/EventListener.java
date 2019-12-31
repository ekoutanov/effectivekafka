package effectivekafka.framework.receiver;

@FunctionalInterface
public interface EventListener<P> {
  void onEvent(ReceiveEvent<? extends P> event);
}
