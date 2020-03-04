package effectivekafka.quota;

import static java.lang.System.*;

import org.apache.kafka.clients.producer.*;

public final class BufferedQuotaProducerSample {
  public static void main(String[] args) throws InterruptedException {
    final var topic = "volume-test";

    final var config = new ScramProducerConfig()
        .withBootstrapServers("localhost:9094")
        .withUsername("alice")
        .withPassword("alice-secret")
        .withClientId("pump")
        .withCustomEntry(ProducerConfig.BUFFER_MEMORY_CONFIG,
                         100_000);

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

        final var record = new ProducerRecord<>(topic, key, value);
        final var tookMs = timed(() -> {
          producer.send(record, callback);
        });
        out.format("Blocked for %,d ms%n", tookMs);
        statsPrinter.maybePrintStats();
      }
    }
  }
  
  private static long timed(Runnable task) {
    final var start = System.currentTimeMillis();
    task.run();
    return System.currentTimeMillis() - start;
  }
}
