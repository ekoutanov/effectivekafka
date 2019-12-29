package effectivekafka.layeredconsumer;

import java.util.*;
import java.util.concurrent.*;

import effectivekafka.layeredconsumer.EventListener;
import effectivekafka.layeredconsumer.event.*;

public abstract class AbstractReceiver implements EventReceiver {
  private final Set<EventListener> listeners = new CopyOnWriteArraySet<>();
  
  public final void addListener(EventListener listener) {
    listeners.add(listener);
  }
  
  protected final void fire(CustomerEvent event, ReceiveError error) {
    for (var listener : listeners) {
      listener.onEvent(event, error);
    }
  }
}
