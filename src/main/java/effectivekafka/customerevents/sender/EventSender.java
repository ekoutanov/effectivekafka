package effectivekafka.customerevents.sender;

import java.io.*;
import java.util.concurrent.*;

import org.apache.kafka.clients.producer.*;

import effectivekafka.customerevents.event.*;

public interface EventSender extends Closeable {
  Future<RecordMetadata> send(CustomerPayload payload);
  
  final class SendException extends Exception {
    private static final long serialVersionUID = 1L;
    
    SendException(Throwable cause) { super(cause); }
  }
  
  default RecordMetadata blockingSend(CustomerPayload payload) throws SendException, InterruptedException {
    try {
      return send(payload).get();
    } catch (ExecutionException e) {
      throw new SendException(e.getCause());
    }
  }
  
  @Override
  public void close();
}
