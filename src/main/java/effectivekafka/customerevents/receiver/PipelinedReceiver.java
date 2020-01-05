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
  private final WorkerThread pollingThread;
  
  private final WorkerThread processingThread;
  
  private final Consumer<String, CustomerPayloadOrError> consumer;
  
  private final Duration pollTimeout;
  
  private final BlockingQueue<ReceiveEvent> receivedEvents;
  
  private final Queue<Map<TopicPartition, OffsetAndMetadata>> pendingOffsets = new LinkedBlockingQueue<>();
  
  public PipelinedReceiver(Map<String, Object> consumerConfig, 
                           String topic, 
                           Duration pollTimeout, 
                           int queueCapacity) {
    this.pollTimeout = pollTimeout;
    receivedEvents = new LinkedBlockingQueue<>(queueCapacity);
    
    final var combinedConfig = new HashMap<String, Object>();
    combinedConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    combinedConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, CustomerPayloadDeserializer.class.getName());
    combinedConfig.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
    combinedConfig.putAll(consumerConfig);
    consumer = new KafkaConsumer<>(combinedConfig);
    consumer.subscribe(Set.of(topic));
    
    pollingThread = WorkerThread.builder()
        .withOptions(new WorkerOptions().daemon().withName(PipelinedReceiver.class, "poller"))
        .onCycle(this::onPollCycle)
        .build();
    
    processingThread = WorkerThread.builder()
        .withOptions(new WorkerOptions().daemon().withName(PipelinedReceiver.class, "processor"))
        .onCycle(this::onProcessCycle)
        .build();
  }
  
  @Override
  public void start() {
    pollingThread.start();
    processingThread.start();
  }
  
  private void onPollCycle(WorkerThread t) throws InterruptedException {
    final ConsumerRecords<String, CustomerPayloadOrError> records;
    
    try {
      records = consumer.poll(pollTimeout);
    } catch (InterruptException e) {
      throw new InterruptedException("Interrupted during poll");
    }
    
    if (! records.isEmpty()) {
      for (var record : records) {
        final var value = record.value();
        final var event = new ReceiveEvent(value.getPayload(), value.getError(), record, value.getEncodedValue());
        receivedEvents.put(event);
      }
    }
    
    for (Map<TopicPartition, OffsetAndMetadata> pendingOffset; 
        (pendingOffset = pendingOffsets.poll()) != null;) {
      consumer.commitAsync(pendingOffset, null);
    }
  }
  
  private void onProcessCycle(WorkerThread t) throws InterruptedException {
    final var event = receivedEvents.take();
    fire(event);
    final var record = event.getRecord();
    pendingOffsets.add(Map.of(new TopicPartition(record.topic(), record.partition()), 
                              new OffsetAndMetadata(record.offset() + 1)));
  }
  
  @Override
  public void close() {
    Terminator.of(pollingThread, processingThread).terminate().joinSilently();
    consumer.close();
  }
}
