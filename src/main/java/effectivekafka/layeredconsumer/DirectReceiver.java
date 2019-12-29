package effectivekafka.layeredconsumer;

import java.time.*;
import java.util.*;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.errors.*;
import org.apache.kafka.common.serialization.*;

import com.obsidiandynamics.worker.*;

import effectivekafka.layeredconsumer.event.*;

public final class DirectReceiver extends AbstractReceiver {
  private final WorkerThread worker;
  
  private final Consumer<String, String> consumer;
  
  private final Duration pollTimeout;
  
  public DirectReceiver(Map<String, Object> consumerConfig, String topic, Duration pollTimeout) {
    this.pollTimeout = pollTimeout;
    
    final var combinedConfig = new HashMap<String, Object>();
    combinedConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    combinedConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    combinedConfig.putAll(consumerConfig);
    consumer = new KafkaConsumer<>(combinedConfig);
    consumer.subscribe(Set.of(topic));
    
    worker = WorkerThread.builder()
        .withOptions(new WorkerOptions().daemon().withName(DirectReceiver.class))
        .onCycle(this::onCycle)
        .build();
  }
  
  @Override
  public void start() {
    worker.start();
  }
  
  private void onCycle(WorkerThread t) throws InterruptedException {
    final ConsumerRecords<String, String> records;
    
    try {
      records = consumer.poll(pollTimeout);
    } catch (InterruptException e) {
      throw new InterruptedException("Interrupted during poll");
    }
    
    if (! records.isEmpty()) {
      for (var record : records) {
        final var unmarshalled = CustomerUnmarshaller.unmarshal(record);
        fire(unmarshalled.getEvent(), unmarshalled.getError());
      }
      consumer.commitAsync();
    }
  }
  
  @Override
  public void close() {
    worker.terminate().joinSilently();
    consumer.close();
  }
}
