package effectivekafka.layeredconsumer;

import java.time.*;
import java.util.*;

import org.apache.kafka.clients.consumer.*;

public final class RunDirectConsumer {
  public static void main(String[] args) throws InterruptedException {
    final var consumerConfig = 
        Map.<String, Object>of(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092",
                               ConsumerConfig.GROUP_ID_CONFIG, "customer-consumer",
                               ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    
    try (var receiver = new DirectReceiver(consumerConfig, "customer.test", Duration.ofMillis(100))) {
      new CustomerBusinessLogic(receiver);
      receiver.start();
      Thread.sleep(10_000);
    }
  }
}
