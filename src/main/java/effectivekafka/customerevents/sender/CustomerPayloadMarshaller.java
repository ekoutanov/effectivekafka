package effectivekafka.customerevents.sender;

import org.apache.kafka.clients.producer.*;

import com.fasterxml.jackson.core.*;
import com.fasterxml.jackson.databind.*;

import effectivekafka.customerevents.event.*;

public final class CustomerPayloadMarshaller {
  private static final ObjectMapper objectMapper = new ObjectMapper();

  private CustomerPayloadMarshaller() {}
  
  public static final class MarshallingException extends RuntimeException {
    private static final long serialVersionUID = 1L;
    
    private MarshallingException(Throwable cause) { super(cause); }
  }
  
  public static ProducerRecord<String, String> marshal(String topic, CustomerPayload payload) {
    final String payloadJson;
    try {
      payloadJson = objectMapper.writeValueAsString(payload);
    } catch (JsonProcessingException e) {
      throw new MarshallingException(e);
    }
    return new ProducerRecord<>(topic, payload.getId().toString(), payloadJson);
  }
}
