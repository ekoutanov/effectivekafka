package effectivekafka.receiver;

import java.io.*;
import java.util.concurrent.*;

import com.obsidiandynamics.worker.*;

public final class AsyncReceiver<P> implements EventReceiver<P>, EventListener<P>, Closeable {
  private final FanoutReceiver<P> fanout = new FanoutReceiver<>();
  
  private final WorkerThread worker;
  
  private final ArrayBlockingQueue<ReceiveEvent<? extends P>> queue;
  
  public AsyncReceiver(String name, int capacity) {
    queue = new ArrayBlockingQueue<>(capacity);
    worker = WorkerThread.builder()
        .onCycle(this::onCycle)
        .withOptions(new WorkerOptions().daemon().withName(AsyncReceiver.class, name))
        .buildAndStart();
  }

  @Override
  public void addListener(EventListener<? super P> listener) {
    fanout.addListener(listener);
  }
  
  private void onCycle(WorkerThread t) throws InterruptedException {
    fanout.onEvent(queue.take());
  }

  @Override
  public void onEvent(ReceiveEvent<? extends P> event) {
    queue.add(event);
  }

  @Override
  public void close() {
    worker.terminate().joinSilently();
  }
}
