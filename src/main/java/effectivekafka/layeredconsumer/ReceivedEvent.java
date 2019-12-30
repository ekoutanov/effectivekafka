package effectivekafka.layeredconsumer;

import org.apache.kafka.clients.consumer.*;

import com.fasterxml.jackson.databind.*;

import effectivekafka.layeredconsumer.event.*;

public final class ReceivedEvent {
  private final CustomerPayload payload;
  
  private final ConsumerRecord<String, String> record;
  
  private final Throwable error;

  private ReceivedEvent(CustomerPayload payload, ConsumerRecord<String, String> record, Throwable error) {
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

  public Throwable getError() {
    return error;
  }

  @Override
  public String toString() {
    return ReceivedEvent.class.getSimpleName() + " [payload=" + payload + ", record=" + record + ", error=" + error + "]";
  }
  
  public static ReceivedEvent payload(CustomerPayload payload, ConsumerRecord<String, String> record) {
    return new ReceivedEvent(payload, record, null);
  }
  
  public static ReceivedEvent error(Throwable error, ConsumerRecord<String, String> record) {
    return new ReceivedEvent(null, record, error);
  }
  
  public static ReceivedEvent unmarshal(ConsumerRecord<String, String> record) {
    try {
      return payload(new ObjectMapper().readValue(record.value(), CustomerPayload.class), record);
    } catch (Throwable e) {
      return error(e, record);
    }
  }
}
