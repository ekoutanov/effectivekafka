package effectivekafka.customerevents.receiver;

import java.time.*;
import java.util.*;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.errors.*;
import org.apache.kafka.common.serialization.*;

import com.obsidiandynamics.worker.*;

public final class DirectReceiver extends AbstractReceiver {
  private final WorkerThread pollingThread;
  
  private final Consumer<String, CustomerPayloadOrError> consumer;
  
  private final Duration pollTimeout;
  
  public DirectReceiver(Map<String, Object> consumerConfig, 
                        String topic, 
                        Duration pollTimeout) {
    this.pollTimeout = pollTimeout;
    
    final var mergedConfig = new HashMap<String, Object>();
    mergedConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    mergedConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, CustomerPayloadDeserializer.class.getName());
    mergedConfig.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
    mergedConfig.putAll(consumerConfig);
    consumer = new KafkaConsumer<>(mergedConfig);
    consumer.subscribe(Set.of(topic));
    
    pollingThread = WorkerThread.builder()
        .withOptions(new WorkerOptions().daemon().withName(DirectReceiver.class, "poller"))
        .onCycle(this::onPollCycle)
        .build();
  }
  
  @Override
  public void start() {
    pollingThread.start();
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
        final var payloadOrError = record.value();
        final var event = new ReceiveEvent(payloadOrError.getPayload(), 
                                           payloadOrError.getError(),
                                           record,
                                           payloadOrError.getEncodedValue());
        fire(event);
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
