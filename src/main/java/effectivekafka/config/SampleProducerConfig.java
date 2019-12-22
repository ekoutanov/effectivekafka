package effectivekafka.config;

import java.util.*;

import org.apache.kafka.clients.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.*;

public final class SampleProducerConfig extends AbstractClientConfig<SampleProducerConfig> {
  private String bootstrapServers;
  
  private Class<? extends Serializer<?>> keySerializerClass;
  
  private Class<? extends Serializer<?>> valueSerializerClass;
  
  public SampleProducerConfig withBootstrapServers(String bootstrapServers) {
    this.bootstrapServers = bootstrapServers;
    return this;
  }
  
  public SampleProducerConfig withKeySerializerClass(Class<? extends Serializer<?>> keySerializerClass) {
    this.keySerializerClass = keySerializerClass;
    return this;
  }

  public SampleProducerConfig withValueSerializerClass(Class<? extends Serializer<?>> valueSerializerClass) {
    this.valueSerializerClass = valueSerializerClass;
    return this;
  }

  @Override
  protected Class<?>[] getValidationClasses() {
    return new Class<?>[] {CommonClientConfigs.class, ProducerConfig.class};
  }

  @Override
  protected void appendExpectedEntries(ExpectedEntryAppender expectedEntries) {
    Objects.requireNonNull(bootstrapServers, "Bootstrap servers not set");
    expectedEntries.append(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    Objects.requireNonNull(keySerializerClass, "Key serializer not set");
    expectedEntries.append(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, keySerializerClass.getName());
    Objects.requireNonNull(valueSerializerClass, "Value serializer not set");
    expectedEntries.append(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, valueSerializerClass.getName());
  }
}
