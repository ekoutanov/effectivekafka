package effectivekafka.customerevents.receiver;

import org.apache.kafka.clients.consumer.*;

import effectivekafka.customerevents.event.*;

public final class ReceiveEvent {
  private final CustomerPayload payload;
  
  private final ConsumerRecord<String, String> record;
  
  private final Throwable error;

  private ReceiveEvent(CustomerPayload payload, ConsumerRecord<String, String> record, Throwable error) {
    this.payload = payload;
    this.record = record;
    this.error = error;
  }

  public CustomerPayload getPayload() {
    return payload;
  }

  public ConsumerRecord<String, String> getRecord() {
    return record;
  }
  
  public boolean isError() {
    return error != null;
  }

  public Throwable getError() {
    return error;
  }

  @Override
  public String toString() {
    return ReceiveEvent.class.getSimpleName() + " [payload=" + payload + ", record=" + record + ", error=" + error + "]";
  }
  
  public static ReceiveEvent success(CustomerPayload payload, ConsumerRecord<String, String> record) {
    return new ReceiveEvent(payload, record, null);
  }
  
  public static ReceiveEvent error(Throwable error, ConsumerRecord<String, String> record) {
    return new ReceiveEvent(null, record, error);
  }
}
