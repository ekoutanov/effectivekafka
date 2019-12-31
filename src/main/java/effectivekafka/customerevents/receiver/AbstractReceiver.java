package effectivekafka.customerevents.receiver;

import java.util.*;

public abstract class AbstractReceiver implements EventReceiver {
  private final Set<EventListener> listeners = new HashSet<>();
  
  public final void addListener(EventListener listener) {
    listeners.add(listener);
  }
  
  protected final void fire(ReceiveEvent event) {
    for (var listener : listeners) {
      listener.onEvent(event);
    }
  }
}
