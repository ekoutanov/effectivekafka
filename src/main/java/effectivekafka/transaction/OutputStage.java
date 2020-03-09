package effectivekafka.transaction;

import static java.lang.System.*;

import java.time.*;
import java.util.*;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.*;

public final class OutputStage {
  public static void main(String[] args) {
    final var topic = "tx-output";
    final var groupId = "output-stage";

    final Map<String, Object> config = 
        Map.of(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092", 
               ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName(), 
               ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class.getName(), 
               ConsumerConfig.GROUP_ID_CONFIG, groupId,
               ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
               ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false,
               ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");

    try (var consumer = new KafkaConsumer<String, Integer>(config)) {
      consumer.subscribe(Set.of(topic));

      while (true) {
        final var records = consumer.poll(Duration.ofMillis(100));
        for (var record : records) {
          out.format("Got record with key %s, value %d%n", 
                     record.key(), record.value());
        }
        consumer.commitAsync();
      }
    }
  }
}
