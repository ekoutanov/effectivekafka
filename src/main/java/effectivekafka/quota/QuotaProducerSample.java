package effectivekafka.quota;

import org.apache.kafka.clients.producer.*;

public final class QuotaProducerSample {
  public static void main(String[] args) throws InterruptedException {
    final var topic = "volume-test";

    final var config = new ScramProducerConfig()
        .withBootstrapServers("localhost:9094")
        .withUsername("alice")
        .withPassword("alice-secret")
        .withClientId("pump");

    final var props = config.mapify();
    try (var producer = new KafkaProducer<String, String>(props)) {
      final var statsPrinter = new StatsPrinter();

      final var key = "some_key";
      final var value = "some_value".repeat(1000);

      while (true) {
        final Callback callback = (metadata, exception) -> {
          statsPrinter.accumulateRecord();
          if (exception != null) exception.printStackTrace();
        };

        producer.send(new ProducerRecord<>(topic, key, value), callback);
        statsPrinter.maybePrintStats();
      }
    }
  }
}
