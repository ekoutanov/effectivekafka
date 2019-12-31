package effectivekafka.framework.receiver;

import java.util.*;
import java.util.concurrent.*;

public final class FanoutReceiver<P> implements EventReceiver<P>, EventListener<P> {
  private final Set<EventListener<? super P>> listeners = new CopyOnWriteArraySet<>();

  @Override
  public void addListener(EventListener<? super P> listener) {
    Objects.requireNonNull(listener, "Listener cannot be null");
    listeners.add(listener);
  }

  @Override
  public void onEvent(ReceiveEvent<? extends P> event) {
    Objects.requireNonNull(event, "Event cannot be null");
    for (var listener : listeners) {
      listener.onEvent(event);
    }
  }

  @Override
  public void close() {}
}
