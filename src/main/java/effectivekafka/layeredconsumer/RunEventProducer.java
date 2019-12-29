package effectivekafka.layeredconsumer;

import java.util.*;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.*;

import com.fasterxml.jackson.core.*;
import com.fasterxml.jackson.databind.*;

import effectivekafka.layeredconsumer.event.*;

public final class RunEventProducer {
  public static void main(String[] args) throws InterruptedException, JsonProcessingException {
    final var config = 
        Map.<String, Object>of(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092", 
                               ProducerConfig.CLIENT_ID_CONFIG, "customer-producer-sample",
                               ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName(), 
                               ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

    final var topic = "customer.test";
    
    final var objectMapper = new ObjectMapper();
    try (var producer = new KafkaProducer<String, String>(config)) {
      while (true) {
        final var event = new CreateCustomerEvent(UUID.randomUUID(), "Bob", "Brown");
        final var eventJson = objectMapper.writeValueAsString(event);
        System.out.format("Publishing %s%n", eventJson);
        producer.send(new ProducerRecord<>(topic, event.getId().toString(), eventJson));
        Thread.sleep(1000);
      }
    }
  }
}
