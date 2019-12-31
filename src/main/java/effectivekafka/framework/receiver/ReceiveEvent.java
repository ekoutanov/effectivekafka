package effectivekafka.framework.receiver;

import org.apache.kafka.clients.consumer.*;

public final class ReceiveEvent<P> {
  private final P payload;
  
  private final ConsumerRecord<?, ?> record;
  
  private final Throwable error;

  public ReceiveEvent(P payload, ConsumerRecord<?, ?> record, Throwable error) {
    this.payload = payload;
    this.record = record;
    this.error = error;
  }

  public P getPayload() {
    return payload;
  }

  public ConsumerRecord<?, ?> getRecord() {
    return record;
  }

  public Throwable getError() {
    return error;
  }
}
