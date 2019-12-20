package effectivekafka.typesafeproducer;

import static org.junit.Assert.*;

import java.util.*;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.*;
import org.assertj.core.api.*;
import org.junit.*;

import effectivekafka.typesafeproducer.TypesafeProducerConfig.*;

public final class TypesafeProducerConfigTest {
  @Test
  public void testExpectedConfig_notSet() {
    final var config = new TypesafeProducerConfig();
    Assertions.assertThatThrownBy(() -> {
      config.mapify();
    }).isExactlyInstanceOf(NullPointerException.class).hasMessageContaining("Bootstrap servers not set");
  }
  
  @Test
  public void testExpectedConfig_allSet() {
    final var config = new TypesafeProducerConfig()
        .withBootstrapServers("localhost:9092")
        .withKeySerializerClass(IntegerSerializer.class)
        .withValueSerializerClass(StringSerializer.class);
    
    final var expected = Map.of("bootstrap.servers", "localhost:9092",
                                "key.serializer", IntegerSerializer.class.getName(),
                                "value.serializer", StringSerializer.class.getName());
    assertEquals(expected, config.mapify());
  }
  
  @Test
  public void testExpectedAndCustomConfig_noOverlap() {
    final var config = new TypesafeProducerConfig()
        .withBootstrapServers("localhost:9092")
        .withKeySerializerClass(IntegerSerializer.class)
        .withValueSerializerClass(StringSerializer.class)
        .withCustomEntry(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 1);
    
    final var expected = Map.of("bootstrap.servers", "localhost:9092",
                                "key.serializer", IntegerSerializer.class.getName(),
                                "value.serializer", StringSerializer.class.getName(),
                                ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 1);
    assertEquals(expected, config.mapify());
  }
  
  @Test
  public void testCustomConfig_withOverrideAttempt() {
    final var config = new TypesafeProducerConfig()
        .withBootstrapServers("localhost:9092")
        .withKeySerializerClass(IntegerSerializer.class)
        .withValueSerializerClass(StringSerializer.class)
        .withCustomEntry(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    
    Assertions.assertThatThrownBy(() -> {
      config.mapify();
    }).isExactlyInstanceOf(OverriddenPropertyException.class).hasMessageContaining("Property key.serializer cannot be overridden");
  }
  
  @Test
  public void testCustomConfig_unsupportedProperty() {
    final var config = new TypesafeProducerConfig()
        .withBootstrapServers("localhost:9092")
        .withKeySerializerClass(IntegerSerializer.class)
        .withValueSerializerClass(StringSerializer.class)
        .withCustomEntry("foo.bar", "value");
    
    Assertions.assertThatThrownBy(() -> {
      config.mapify();
    }).isExactlyInstanceOf(UnsupportedPropertyException.class).hasMessageContaining("Unsupported property foo.bar");
  }
}
