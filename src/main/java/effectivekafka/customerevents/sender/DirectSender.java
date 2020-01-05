package effectivekafka.customerevents.sender;

import java.util.*;
import java.util.concurrent.*;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.*;

import effectivekafka.customerevents.event.*;

public final class DirectSender implements EventSender {
  private final Producer<String, CustomerPayload> producer;
  
  private final String topic;

  public DirectSender(Map<String, Object> producerConfig, String topic) {
    this.topic = topic;
    
    final var combinedConfig = new HashMap<String, Object>();
    combinedConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    combinedConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, CustomerPayloadSerializer.class.getName());
    combinedConfig.putAll(producerConfig);
    producer = new KafkaProducer<>(combinedConfig);
  }

  @Override
  public Future<RecordMetadata> send(CustomerPayload payload) {
    final var record = new ProducerRecord<>(topic, payload.getId().toString(), payload);
    return producer.send(record);
  }

  @Override
  public void close() {
    producer.close();
  }
}
