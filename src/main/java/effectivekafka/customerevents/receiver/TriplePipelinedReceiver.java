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

public final class TriplePipelinedReceiver extends AbstractReceiver {
  private final WorkerThread ioThread;
  
  private final WorkerThread unmarshallingThread;
  
  private final WorkerThread processingThread;
  
  private final Consumer<String, String> consumer;
  
  private final Duration pollTimeout;
  
  private final BlockingQueue<ConsumerRecord<String, String>> fetchedRecords;
  
  private final BlockingQueue<ReceiveEvent> unmarshalledRecords;
  
  private final Queue<Map<TopicPartition, OffsetAndMetadata>> pendingOffsets = new LinkedBlockingQueue<>();
  
  public TriplePipelinedReceiver(Map<String, Object> consumerConfig, 
                                 String topic, 
                                 Duration pollTimeout, 
                                 int queueCapacity) {
    this.pollTimeout = pollTimeout;
    fetchedRecords = new LinkedBlockingQueue<>(queueCapacity);
    unmarshalledRecords = new LinkedBlockingQueue<>(queueCapacity);
    
    final var combinedConfig = new HashMap<String, Object>();
    combinedConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    combinedConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    combinedConfig.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
    combinedConfig.putAll(consumerConfig);
    consumer = new KafkaConsumer<>(combinedConfig);
    consumer.subscribe(Set.of(topic));
    
    ioThread = WorkerThread.builder()
        .withOptions(new WorkerOptions().daemon().withName(TriplePipelinedReceiver.class, "io"))
        .onCycle(this::onIoCycle)
        .build();
    
    unmarshallingThread = WorkerThread.builder()
        .withOptions(new WorkerOptions().daemon().withName(TriplePipelinedReceiver.class, "unmarshal"))
        .onCycle(this::onUnmarshalCycle)
        .build();
    
    processingThread = WorkerThread.builder()
        .withOptions(new WorkerOptions().daemon().withName(TriplePipelinedReceiver.class, "processor"))
        .onCycle(this::onProcessorCycle)
        .build();
  }
  
  @Override
  public void start() {
    ioThread.start();
    unmarshallingThread.start();
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
  
  private void onUnmarshalCycle(WorkerThread t) throws InterruptedException {
    final var record = fetchedRecords.take();
    final var unmarshalled = CustomerPayloadUnmarshaller.unmarshal(record);
    unmarshalledRecords.put(unmarshalled);
  }
  
  private void onProcessorCycle(WorkerThread t) throws InterruptedException {
    final var unmarshalled = unmarshalledRecords.take();
    fire(unmarshalled);
    final var record = unmarshalled.getRecord();
    pendingOffsets.add(Map.of(new TopicPartition(record.topic(), record.partition()), 
                              new OffsetAndMetadata(record.offset() + 1)));
  }
  
  @Override
  public void close() {
    Terminator.of(ioThread, unmarshallingThread, processingThread).terminate().joinSilently();
    consumer.close();
  }
}
