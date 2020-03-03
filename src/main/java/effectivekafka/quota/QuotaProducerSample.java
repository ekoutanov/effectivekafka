package effectivekafka.quota;

import static java.lang.System.*;

import java.util.*;
import java.util.concurrent.atomic.*;

import org.apache.kafka.clients.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.config.*;
import org.apache.kafka.common.security.scram.*;
import org.apache.kafka.common.serialization.*;

public final class QuotaProducerSample {
  public static void main(String[] args) throws InterruptedException {
    final var topic = "volume-test";

    final var loginModuleClass = ScramLoginModule.class.getName();
    final var saslJaasConfig = loginModuleClass 
        + " required\n"
        + "username=\"alice\"\n"
        + "password=\"alice-secret\";";
    
    final Map<String, Object> config = Map
        .of(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9094", 
            CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_SSL",
            SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, "https",
            SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, "client.truststore.jks",
            SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, "secret",
            SaslConfigs.SASL_MECHANISM, "SCRAM-SHA-512",
            SaslConfigs.SASL_JAAS_CONFIG, saslJaasConfig,
            ProducerConfig.CLIENT_ID_CONFIG, "pump",
            ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName(), 
            ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    
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
        producer.send(new ProducerRecord<>(topic, KEY, VALUE), callback);
        statsPrinter.maybePrintStats();
      }
    }
  }
  
  private interface BackpressureHandler {
    void exert() throws InterruptedException;
  }
  
  private static class Backpressure {
    static final int MAX_PENDING_RECORDS = 10;
    
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
  
  private static class StatsPrinter {
    static final long PRINT_INTERVAL_MS = 1_000;
    
    long timestampOfLastPrint = System.currentTimeMillis();
    long lastNumberOfRecords = 0;
    long totalNumberOfRecords = 0;
    
    void accumulateRecord() {
      totalNumberOfRecords++;
    }
    
    void maybePrintStats() {
      final var now = System.currentTimeMillis();
      final var timeElapsed = now - timestampOfLastPrint;
      if (timeElapsed > PRINT_INTERVAL_MS) {
        final var produced = totalNumberOfRecords - lastNumberOfRecords;
        final var rate = produced / (double) timeElapsed * 1000d;
        out.printf("Rate: %,.0f/sec%n", rate);
        lastNumberOfRecords = totalNumberOfRecords;
        timestampOfLastPrint = now;
      }
    }
  }
}
