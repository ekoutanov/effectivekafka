package effectivekafka.customerevents.receiver;

import org.apache.kafka.common.serialization.*;

import com.fasterxml.jackson.databind.*;

import effectivekafka.customerevents.event.*;

public final class CustomerPayloadDeserializer implements Deserializer<CustomerPayloadOrError> {
  private final ObjectMapper objectMapper = new ObjectMapper();

  @Override
  public CustomerPayloadOrError deserialize(String topic, byte[] data) {
    final var value = new String(data);
    try {
      final var payload = objectMapper.readValue(value, CustomerPayload.class);
      return new CustomerPayloadOrError(payload, null, value);
    } catch (Throwable e) {
      return new CustomerPayloadOrError(null, e, value);
    }
  }
}
