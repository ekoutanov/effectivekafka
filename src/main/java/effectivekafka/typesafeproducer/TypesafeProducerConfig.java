package effectivekafka.typesafeproducer;

import static java.util.function.Predicate.*;

import java.lang.reflect.*;
import java.util.*;
import java.util.stream.*;

import org.apache.kafka.clients.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.*;

public final class TypesafeProducerConfig {
  private String bootstrapServers;
  
  private Class<? extends Serializer<?>> keySerializerClass;
  
  private Class<? extends Serializer<?>> valueSerializerClass;
  
  private Map<String, Object> customEntries = new HashMap<>();
  
  public TypesafeProducerConfig withBootstrapServers(String bootstrapServers) {
    this.bootstrapServers = bootstrapServers;
    return this;
  }
  
  public TypesafeProducerConfig withKeySerializerClass(Class<? extends Serializer<?>> keySerializerClass) {
    this.keySerializerClass = keySerializerClass;
    return this;
  }

  public TypesafeProducerConfig withValueSerializerClass(Class<? extends Serializer<?>> valueSerializerClass) {
    this.valueSerializerClass = valueSerializerClass;
    return this;
  }

  public TypesafeProducerConfig withCustomEntry(String propertyName, Object value) {
    Objects.requireNonNull(propertyName, "Property name cannot be null");
    customEntries.put(propertyName, value);
    return this;
  }
  
  public static final class UnsupportedPropertyException extends RuntimeException {
    private static final long serialVersionUID = 1L;
    
    private UnsupportedPropertyException(String s) { super(s); }
  }

  public Map<String, Object> mapify() {
    final var staging = new HashMap<String, Object>();
    if (! customEntries.isEmpty()) {
      final var supportedKeys = scanClassesForKeys(CommonClientConfigs.class, ProducerConfig.class);
      final var unsupportedKey = customEntries.keySet()
          .stream()
          .filter(not(supportedKeys::contains))
          .findAny();
      
      if (unsupportedKey.isPresent()) {
        throw new UnsupportedPropertyException("Unsupported property " + unsupportedKey.get());
      }
      
      staging.putAll(customEntries);
    }

    Objects.requireNonNull(bootstrapServers, "Bootstrap servers not set");
    tryInsertEntry(staging, CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    Objects.requireNonNull(keySerializerClass, "Key serializer not set");
    tryInsertEntry(staging, ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, keySerializerClass.getName());
    Objects.requireNonNull(valueSerializerClass, "Value serializer not set");
    tryInsertEntry(staging, ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, valueSerializerClass.getName());
    
    return staging;
  }
  
  public static final class OverriddenPropertyException extends RuntimeException {
    private static final long serialVersionUID = 1L;
    
    private OverriddenPropertyException(String s) { super(s); }
  }
  
  private static void tryInsertEntry(Map<String, Object> staging, String key, Object value) {
    staging.compute(key, (__key, existingValue) -> {
      if (existingValue == null) {
        return value;
      } else {
        throw new OverriddenPropertyException("Property " + key + " cannot be overridden");
      }
    });
  }
  
  private static Set<String> scanClassesForKeys(Class<?>... classes) {
    return Arrays.stream(classes)
        .map(Class::getFields)
        .flatMap(Arrays::stream)
        .filter(TypesafeProducerConfig::isFieldPublicConstant)
        .filter(TypesafeProducerConfig::isFieldStringType)
        .filter(not(TypesafeProducerConfig::isFieldDoc))
        .map(TypesafeProducerConfig::retrieveField)
        .collect(Collectors.toSet());
  }
  
  private static boolean isFieldPublicConstant(Field field) {
    final var modifiers = field.getModifiers();
    return Modifier.isFinal(modifiers) && Modifier.isPublic(modifiers) && Modifier.isStatic(modifiers);
  }
  
  private static boolean isFieldStringType(Field field) {
    return field.getType().equals(String.class);
  }
  
  private static boolean isFieldDoc(Field field) {
    return field.getName().endsWith("_DOC");
  }
  
  private static String retrieveField(Field field) {
    try {
      return (String) field.get(null);
    } catch (IllegalArgumentException | IllegalAccessException e) {
      throw new RuntimeException(e);
    }
  }
}
