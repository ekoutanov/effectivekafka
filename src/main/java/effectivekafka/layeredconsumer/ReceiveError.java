package effectivekafka.layeredconsumer;

import org.apache.kafka.clients.consumer.*;

public final class ReceiveError {
  private final ConsumerRecord<String, String> record;
  
  private final Throwable exception;

  public ReceiveError(ConsumerRecord<String, String> record, Throwable exception) {
    this.record = record;
    this.exception = exception;
  }

  public ConsumerRecord<String, String> getRecord() {
    return record;
  }

  public Throwable getException() {
    return exception;
  }
}
