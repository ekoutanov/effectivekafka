package effectivekafka.customerevents.receiver;

import java.time.*;
import java.util.*;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.errors.*;
import org.apache.kafka.common.serialization.*;

import com.obsidiandynamics.worker.*;

public final class DirectReceiver extends AbstractReceiver {
  private final WorkerThread pollingThread;
  
  private final Consumer<String, String> consumer;
  
  private final Duration pollTimeout;
  
  public DirectReceiver(Map<String, Object> consumerConfig, 
                        String topic, 
                        Duration pollTimeout) {
    this.pollTimeout = pollTimeout;
    
    final var combinedConfig = new HashMap<String, Object>();
    combinedConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    combinedConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    combinedConfig.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
    combinedConfig.putAll(consumerConfig);
    consumer = new KafkaConsumer<>(combinedConfig);
    consumer.subscribe(Set.of(topic));
    
    pollingThread = WorkerThread.builder()
        .withOptions(new WorkerOptions().daemon().withName(DirectReceiver.class, "poller"))
        .onCycle(this::onPollerCycle)
        .build();
  }
  
  @Override
  public void start() {
    pollingThread.start();
  }
  
  private void onPollerCycle(WorkerThread t) throws InterruptedException {
    final ConsumerRecords<String, String> records;
    
    try {
      records = consumer.poll(pollTimeout);
    } catch (InterruptException e) {
      throw new InterruptedException("Interrupted during poll");
    }
    
    if (! records.isEmpty()) {
      for (var record : records) {
        fire(CustomerPayloadUnmarshaller.unmarshal(record));
      }
      consumer.commitAsync();
    }
  }
  
  @Override
  public void close() {
    pollingThread.terminate().joinSilently();
    consumer.close();
  }
}
