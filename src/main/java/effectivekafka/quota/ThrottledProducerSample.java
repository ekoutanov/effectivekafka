package effectivekafka.quota;

import java.util.concurrent.atomic.*;

import org.apache.kafka.clients.producer.*;

public final class ThrottledProducerSample {
  public static void main(String[] args) throws InterruptedException {
    final var topic = "volume-test";

    final var config = new ScramProducerConfig()
        .withBootstrapServers("localhost:9094")
        .withUsername("alice")
        .withPassword("alice-secret")
        .withClientId("pump");

    final var props = config.mapify();
    try (var producer = new KafkaProducer<String, String>(props)) {
      final var backpressure = new Backpressure();
      final var statsPrinter = new StatsPrinter();

      final var key = "some_key";
      final var value = "some_value".repeat(1000);

      while (true) {
        backpressure.maybeApply(() -> {
          Thread.sleep(1);
          statsPrinter.maybePrintStats();
        });

        final Callback callback = (metadata, exception) -> {
          backpressure.clearRecord();
          statsPrinter.accumulateRecord();
          if (exception != null) exception.printStackTrace();
        };
        backpressure.queueRecord();

        final var record = new ProducerRecord<>(topic, key, value);
        producer.send(record, callback);
        statsPrinter.maybePrintStats();
      }
    }
  }

  private interface BackpressureHandler {
    void exert() throws InterruptedException;
  }

  private static class Backpressure {
    static final int MAX_PENDING_RECORDS = 100;

    final AtomicInteger pendingRecords = new AtomicInteger();

    void queueRecord() {
      pendingRecords.incrementAndGet();
    }

    void clearRecord() {
      pendingRecords.decrementAndGet();
    }

    void maybeApply(BackpressureHandler handler) throws InterruptedException {
      if (pendingRecords.get() > MAX_PENDING_RECORDS) {
        do {
          handler.exert();
        } while (pendingRecords.get() > MAX_PENDING_RECORDS / 2);
      }
    }
  }
}
