package effectivekafka.customerevents.receiver;

import java.time.*;
import java.util.*;
import java.util.concurrent.*;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.*;
import org.apache.kafka.common.errors.*;
import org.apache.kafka.common.serialization.*;

import com.obsidiandynamics.worker.*;
import com.obsidiandynamics.worker.Terminator;

public final class PipelinedReceiver extends AbstractReceiver {
  private final WorkerThread ioThread;
  
  private final WorkerThread processingThread;
  
  private final Consumer<String, String> consumer;
  
  private final Duration pollTimeout;
  
  private final BlockingQueue<ConsumerRecord<String, String>> fetchedRecords;
  
  private final Queue<Map<TopicPartition, OffsetAndMetadata>> pendingOffsets = new LinkedBlockingQueue<>();
  
  public PipelinedReceiver(Map<String, Object> consumerConfig, 
                           String topic, 
                           Duration pollTimeout, 
                           int queueCapacity) {
    this.pollTimeout = pollTimeout;
    fetchedRecords = new LinkedBlockingQueue<>(queueCapacity);
    
    final var combinedConfig = new HashMap<String, Object>();
    combinedConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    combinedConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    combinedConfig.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
    combinedConfig.putAll(consumerConfig);
    consumer = new KafkaConsumer<>(combinedConfig);
    consumer.subscribe(Set.of(topic));
    
    ioThread = WorkerThread.builder()
        .withOptions(new WorkerOptions().daemon().withName(PipelinedReceiver.class, "io"))
        .onCycle(this::onIoCycle)
        .build();
    
    processingThread = WorkerThread.builder()
        .withOptions(new WorkerOptions().daemon().withName(PipelinedReceiver.class, "processor"))
        .onCycle(this::onProcessorCycle)
        .build();
  }
  
  @Override
  public void start() {
    ioThread.start();
    processingThread.start();
  }
  
  private void onIoCycle(WorkerThread t) throws InterruptedException {
    final ConsumerRecords<String, String> records;
    
    try {
      records = consumer.poll(pollTimeout);
    } catch (InterruptException e) {
      throw new InterruptedException("Interrupted during poll");
    }
    
    if (! records.isEmpty()) {
      for (var record : records) {
        fetchedRecords.put(record);
      }
    }
    
    for (Map<TopicPartition, OffsetAndMetadata> pendingOffset; 
        (pendingOffset = pendingOffsets.poll()) != null;) {
      consumer.commitAsync(pendingOffset, null);
    }
  }
  
  private void onProcessorCycle(WorkerThread t) throws InterruptedException {
    final var record = fetchedRecords.take();
    final var unmarshalled = CustomerPayloadUnmarshaller.unmarshal(record);
    fire(unmarshalled);
    pendingOffsets.add(Map.of(new TopicPartition(record.topic(), record.partition()), 
                              new OffsetAndMetadata(record.offset() + 1)));
  }
  
  @Override
  public void close() {
    Terminator.of(ioThread, processingThread).terminate().joinSilently();
    consumer.close();
  }
}
