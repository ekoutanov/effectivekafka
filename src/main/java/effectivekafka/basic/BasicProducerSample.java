package effectivekafka.basic;

import java.util.*;

import org.apache.kafka.clients.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.*;

public final class BasicProducerSample {
  public static void main(String[] args) throws InterruptedException {
    final Map<String, Object> config = 
        Map.of(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092", 
               CommonClientConfigs.CLIENT_ID_CONFIG, "basic-consumer-sample",
               ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName(), 
               ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName(), 
               ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 1);

    try (Producer<String, String> producer = new KafkaProducer<>(config)) {
      while (true) {
        final String value = new Date().toString();
        System.out.format("Publishing record with value %s%n", value);
        producer.send(new ProducerRecord<>("test", "myKey", value));
        Thread.sleep(1000);
      }
    }
  }
}
