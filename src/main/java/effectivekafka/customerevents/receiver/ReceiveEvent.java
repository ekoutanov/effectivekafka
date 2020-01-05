package effectivekafka.customerevents.receiver;

import org.apache.kafka.clients.consumer.*;

import effectivekafka.customerevents.event.*;

public final class ReceiveEvent {
  private final CustomerPayload payload;
  
  private final Throwable error;
  
  private final ConsumerRecord<String, ?> record;
  
  private final String encodedValue;

  public ReceiveEvent(CustomerPayload payload, Throwable error, ConsumerRecord<String, ?> record, String encodedValue) {
    this.record = record;
    this.payload = payload;
    this.error = error;
    this.encodedValue = encodedValue;
  }

  public CustomerPayload getPayload() {
    return payload;
  }
  
  public boolean isError() {
    return error != null;
  }

  public Throwable getError() {
    return error;
  }

  public ConsumerRecord<String, ?> getRecord() {
    return record;
  }
  
  public String getEncodedValue() {
    return encodedValue;
  }

  @Override
  public String toString() {
    return ReceiveEvent.class.getSimpleName() + " [payload=" + payload + ", error=" + error + 
        ", record=" + record + ", encodedValue=" + encodedValue + "]";
  }
}
