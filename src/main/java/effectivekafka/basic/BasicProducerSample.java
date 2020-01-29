package effectivekafka.basic;

import java.util.*;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.*;

public final class BasicProducerSample {
  public static void main(String[] args) throws InterruptedException {
    final var topic = "getting-started";
    
    final var config = 
        Map.<String, Object> of(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092", 
               ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName(), 
               ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName(), 
               ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);

    try (var producer = new KafkaProducer<String, String>(config)) {
      while (true) {
        final var key = "myKey";
        final var value = new Date().toString();
        System.out.format("Publishing record with value %s%n", value);
        
        // publish the record, handling the metadata in the callback
        producer.send(new ProducerRecord<>(topic, key, value), (metadata, exception) -> {
          System.out.format("Published with metadata: %s, error: %s%n", metadata, exception);
        });
        
        // wait before publishing another
        Thread.sleep(1000);
      }
    }
  }
}
