package effectivekafka.quota;

import java.util.*;
import java.util.concurrent.atomic.*;

import org.apache.kafka.clients.*;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.config.*;
import org.apache.kafka.common.security.scram.*;
import org.apache.kafka.common.serialization.*;

public final class ThrottledProducerSample {
  public static void main(String[] args) throws InterruptedException {
    final var topic = "volume-test";

    final var loginModuleClass = ScramLoginModule.class.getName();
    final var saslJaasConfig = loginModuleClass 
        + " required\n"
        + "username=\"alice\"\n"
        + "password=\"alice-secret\";";

    final var config = new HashMap<String, Object>();
    config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9094");
    config.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_SSL");
    config.put(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, "https");
    config.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, "client.truststore.jks");
    config.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, "secret");
    config.put(SaslConfigs.SASL_MECHANISM, "SCRAM-SHA-512");
    config.put(SaslConfigs.SASL_JAAS_CONFIG, saslJaasConfig);
    config.put(ProducerConfig.CLIENT_ID_CONFIG, "pump");
    config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

    try (var producer = new KafkaProducer<String, String>(config)) {
      final var backpressure = new Backpressure();
      final var statsPrinter = new StatsPrinter();

      final var KEY = "some_key";
      final var VALUE = "some_value".repeat(1000);

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

        final var record = new ProducerRecord<>(topic, KEY, VALUE);
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
