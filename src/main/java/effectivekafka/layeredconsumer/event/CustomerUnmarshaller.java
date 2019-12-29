package effectivekafka.layeredconsumer.event;

import org.apache.kafka.clients.consumer.*;

import com.fasterxml.jackson.databind.*;

import effectivekafka.layeredconsumer.*;

public final class CustomerUnmarshaller {
  private CustomerUnmarshaller() {}
  
  public static final class Unmarshalled {
    private final CustomerEvent event;
    
    private final ReceiveError error;

    private Unmarshalled(CustomerEvent event, ReceiveError error) {
      this.event = event;
      this.error = error;
    }

    public CustomerEvent getEvent() {
      return event;
    }

    public ReceiveError getError() {
      return error;
    }
    
    public static Unmarshalled event(CustomerEvent event) {
      return new Unmarshalled(event, null);
    }
    
    public static Unmarshalled error(ReceiveError error) {
      return new Unmarshalled(null, error);
    }
  }
  
  public static Unmarshalled unmarshal(ConsumerRecord<String, String> record) {
    try {
      return Unmarshalled.event(new ObjectMapper().readValue(record.value(), CustomerEvent.class));
    } catch (Throwable e) {
      return Unmarshalled.error(new ReceiveError(record, e));
    }
  }
}
