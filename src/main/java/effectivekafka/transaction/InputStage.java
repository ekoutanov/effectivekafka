package effectivekafka.transaction;

import static java.lang.System.*;

import java.util.*;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.*;

public final class InputStage {
  public static void main(String[] args) throws InterruptedException {
    final var topic = "tx-input";

    final Map<String, Object> config = 
        Map.of(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092", 
               ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName(), 
               ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName(), 
               ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);

    try (var producer = new KafkaProducer<String, Integer>(config)) {
      while (true) {
        final var key = new Date().toString();
        final var value = (int) (Math.random() * 1000);
        
        out.format("Publishing record with key %s, value %d%n", 
                   key, value);
        producer.send(new ProducerRecord<>(topic, key, value));
        
        Thread.sleep(500);
      }
    }
  }
}
