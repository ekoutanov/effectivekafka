package effectivekafka.layeredconsumer;

import java.util.*;
import java.util.concurrent.*;

public abstract class AbstractReceiver implements EventReceiver {
  private final Set<EventListener> listeners = new CopyOnWriteArraySet<>();
  
  public final void addListener(EventListener listener) {
    listeners.add(listener);
  }
  
  protected final void fire(ReceivedEvent event) {
    for (var listener : listeners) {
      listener.onEvent(event);
    }
  }
}
