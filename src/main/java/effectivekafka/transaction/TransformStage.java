package effectivekafka.transaction;

import static java.lang.System.*;

import java.time.*;
import java.util.*;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.*;
import org.apache.kafka.common.serialization.*;

public final class TransformStage {
  public static void main(String[] args) {
    final var inputTopic = "tx-input";
    final var outputTopic = "tx-output";
    final var groupId = "transform-stage";

    final Map<String, Object> producerBaseConfig =
        Map.of(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092", 
               ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName(), 
               ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName(), 
               ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);

    final Map<String, Object> consumerConfig = 
        Map.of(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092", 
               ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName(), 
               ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class.getName(), 
               ConsumerConfig.GROUP_ID_CONFIG, groupId,
               ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
               ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
    
    final var producers = new PinnedProducers(producerBaseConfig);

    try (var consumer = new KafkaConsumer<String, Integer>(consumerConfig)) {
      consumer.subscribe(Set.of(inputTopic), producers.rebalanceListener());

      while (true) {
        final var inRecs = consumer.poll(Duration.ofMillis(100));
        
        // read the records, transforming their values
        for (var inRec : inRecs) {
          final var inKey = inRec.key();
          final var inValue = inRec.value();
          out.format("Got record with key %s, value %d%n", 
                     inKey, inValue);

          // prepare the output record
          final var outValue = inValue * inValue;
          final var outRec = 
              new ProducerRecord<>(outputTopic, inKey, outValue);
          final var topicPartition = 
              new TopicPartition(inRec.topic(), inRec.partition());
          
          // acquire producer for the input topic-partition
          final var producer = producers.get(topicPartition);
          
          // transactionally publish record and commit input offsets
          producer.beginTransaction();
          producer.send(outRec);
          try {
            final var nextOffset = 
                new OffsetAndMetadata(inRec.offset() + 1);
            final var offsets = Map.of(topicPartition, nextOffset);
            producer.sendOffsetsToTransaction(offsets, groupId);
            producer.commitTransaction();
          } catch (RuntimeException e) {
            producer.abortTransaction();
            throw e;
          }
        }
      }
    }
  }

  /**
   *  Mapping of producers to input topic-partitions.
   */
  private static class PinnedProducers {
    final Map<String, Object> baseConfig;

    final Map<String, Producer<String, Integer>> producers = new HashMap<>();

    PinnedProducers(Map<String, Object> baseConfig) {
      this.baseConfig = baseConfig;
    }

    ConsumerRebalanceListener rebalanceListener() {
      return new ConsumerRebalanceListener() {
        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
          for (var topicPartition : partitions) {
            out.format("Revoked %s%n", topicPartition);
            disposeProducer(getTransactionalId(topicPartition));
          }
        }

        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
          for (var topicPartition : partitions) {
            out.format("Assigned %s%n", topicPartition);
            createProducer(getTransactionalId(topicPartition));
          }
        }
      };
    }

    Producer<String, Integer> get(TopicPartition topicPartition) {
      final var transactionalId = getTransactionalId(topicPartition);
      final var producer = producers.get(transactionalId);
      Objects.requireNonNull(producer, "No such producer: " + transactionalId);
      return producer;
    }

    String getTransactionalId(TopicPartition topicPartition) {
      return topicPartition.topic() + "-" + topicPartition.partition();
    }

    void createProducer(String transactionalId) {
      if (producers.containsKey(transactionalId))
        throw new IllegalStateException("Producer already exists: " + transactionalId);

      final var config = new HashMap<>(baseConfig);
      config.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, transactionalId);
      final var producer = new KafkaProducer<String, Integer>(config);
      producers.put(transactionalId, producer);
      producer.initTransactions();
    }

    void disposeProducer(String transactionalId) {
      final var producer = producers.remove(transactionalId);
      Objects.requireNonNull(producer, "No such producer: " + transactionalId);
      producer.close();
    }
  }
}
