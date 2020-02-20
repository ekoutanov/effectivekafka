package effectivekafka.ssl;

import static java.lang.System.*;

import java.time.*;
import java.util.*;

import org.apache.kafka.clients.*;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.config.*;
import org.apache.kafka.common.serialization.*;

public final class SslConsumerSample {
  public static void main(String[] args) {
    final var topic = "getting-started";

    final Map<String, Object> config = 
        Map.of(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9093", 
               CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SSL",
               SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, "https",
               SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, "client.truststore.jks",
               SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, "secret",
               ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName(), 
               ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName(), 
               ConsumerConfig.GROUP_ID_CONFIG, "basic-consumer-sample",
               ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
               ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

    try (var consumer = new KafkaConsumer<String, String>(config)) {
      consumer.subscribe(Set.of(topic));

      while (true) {
        final var records = consumer.poll(Duration.ofMillis(100));
        for (var record : records) {
          out.format("Got record with value %s%n", record.value());
        }
        consumer.commitAsync();
      }
    }
  }
}
