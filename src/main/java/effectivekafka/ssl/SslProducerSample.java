package effectivekafka.ssl;

import static java.lang.System.*;

import java.util.*;

import org.apache.kafka.clients.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.config.*;
import org.apache.kafka.common.serialization.*;

public final class SslProducerSample {
  public static void main(String[] args) throws InterruptedException {
    final var topic = "getting-started";

    final Map<String, Object> config = 
        Map.of(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9093", 
               CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SSL",
               SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, "",
               SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, "client.truststore.jks",
               SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, "test1234",
               ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName(), 
               ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName(), 
               ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);

    try (var producer = new KafkaProducer<String, String>(config)) {
      while (true) {
        final var key = "myKey";
        final var value = new Date().toString();
        out.format("Publishing record with value %s%n", 
                   value);

        final Callback callback = (metadata, exception) -> {
          out.format("Published with metadata: %s, error: %s%n", 
                     metadata, exception);
        };

        // publish the record, handling the metadata in the callback
        producer.send(new ProducerRecord<>(topic, key, value), callback);

        // wait a second before publishing another
        Thread.sleep(1000);
      }
    }
  }
}
