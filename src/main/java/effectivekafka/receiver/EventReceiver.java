package effectivekafka.receiver;

import java.io.*;

public interface EventReceiver<P> extends Closeable {
  void addListener(EventListener<? super P> listener);
  
  @Override
  void close();
}
