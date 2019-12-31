package effectivekafka.customerevents.receiver;

import org.apache.kafka.clients.consumer.*;

import com.fasterxml.jackson.databind.*;

import effectivekafka.customerevents.event.*;

public final class CustomerPayloadUnmarshaller {
  private static final ObjectMapper objectMapper = new ObjectMapper();

  private CustomerPayloadUnmarshaller() {}
  
  public static ReceiveEvent unmarshal(ConsumerRecord<String, String> record) {
    try {
      final var payload = objectMapper.readValue(record.value(), CustomerPayload.class);
      return ReceiveEvent.success(payload, record);
    } catch (Throwable e) {
      return ReceiveEvent.error(e, record);
    }
  }
}
