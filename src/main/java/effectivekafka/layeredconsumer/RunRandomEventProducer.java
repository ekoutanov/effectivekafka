package effectivekafka.layeredconsumer;

import java.util.*;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.*;

import com.fasterxml.jackson.core.*;
import com.fasterxml.jackson.databind.*;

import effectivekafka.layeredconsumer.event.*;

public final class RunRandomEventProducer {
  public static void main(String[] args) throws InterruptedException, JsonProcessingException {
    final var config = 
        Map.<String, Object>of(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092", 
                               ProducerConfig.CLIENT_ID_CONFIG, "customer-producer-sample",
                               ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName(), 
                               ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

    final var topic = "customer.test";
    final var objectMapper = new ObjectMapper();
    
    try (var producer = new KafkaProducer<String, String>(config)) {
      final var sender = new Object() {
        void send(CustomerPayload event) throws JsonProcessingException {
          final var eventJson = objectMapper.writeValueAsString(event);
          System.out.format("Publishing %s%n", eventJson);
          producer.send(new ProducerRecord<>(topic, event.getId().toString(), eventJson));
        }
      };
      
      while (true) {
        final var create = new CreateCustomer(UUID.randomUUID(), "Bob", "Brown");
        sender.send(create);
        
        if (Math.random() > 0.5) {
          final var update = new UpdateCustomer(create.getId(), "Charlie", "Brown");
          sender.send(update);
        }
        
        if (Math.random() > 0.5) {
          final var suspend = new SuspendCustomer(create.getId());
          sender.send(suspend);
          
          if (Math.random() > 0.5) {
            final var reinstate = new ReinstateCustomer(create.getId());
            sender.send(reinstate);
          }
        }
        
        Thread.sleep(500);
      }
    }
  }
}
