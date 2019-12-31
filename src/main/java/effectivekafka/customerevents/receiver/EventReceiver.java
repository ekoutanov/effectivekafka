package effectivekafka.customerevents.receiver;

import java.io.*;

public interface EventReceiver extends Closeable {
  void addListener(EventListener listener);
  
  void start();
  
  @Override
  void close();
}
