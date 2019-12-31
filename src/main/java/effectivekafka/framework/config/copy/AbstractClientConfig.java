package effectivekafka.framework.config.copy;

import static java.util.function.Predicate.*;

import java.lang.reflect.*;
import java.util.*;
import java.util.stream.*;

public abstract class AbstractClientConfig<C extends AbstractClientConfig<?>> {
  public static final class UnsupportedPropertyException extends RuntimeException {
    private static final long serialVersionUID = 1L;
    
    private UnsupportedPropertyException(String s) { super(s); }
  }
  
  public static final class ConflictingPropertyException extends RuntimeException {
    private static final long serialVersionUID = 1L;
    
    private ConflictingPropertyException(String s) { super(s); }
  }
  
  private final Map<String, Object> customEntries = new HashMap<>();
  
  @SuppressWarnings("unchecked")
  public final C withCustomEntry(String propertyName, Object value) {
    Objects.requireNonNull(propertyName, "Property name cannot be null");
    customEntries.put(propertyName, value);
    return (C) this;
  }
  
  public final Map<String, Object> mapify() {
    final var stagingConfig = new HashMap<String, Object>();
    if (! customEntries.isEmpty()) {
      final var supportedKeys = scanClassesForPropertyNames(getValidationClasses());
      final var unsupportedKey = customEntries.keySet()
          .stream()
          .filter(not(supportedKeys::contains))
          .findAny();
      
      if (unsupportedKey.isPresent()) {
        throw new UnsupportedPropertyException("Unsupported property " + unsupportedKey.get());
      }
      
      stagingConfig.putAll(customEntries);
    }

    appendExpectedEntries(new ExpectedEntryAppender(stagingConfig));
    return stagingConfig;
  }
  
  protected static final class ExpectedEntryAppender {
    private final Map<String, Object> stagingConfig;

    private ExpectedEntryAppender(Map<String, Object> stagingConfig) {
      this.stagingConfig = stagingConfig;
    }
    
    public void append(String key, Object value) {
      stagingConfig.compute(key, (__key, existingValue) -> {
        if (existingValue == null) {
          return value;
        } else {
          throw new ConflictingPropertyException("Property " + key + " conflicts with an expected property");
        }
      });
    }
  }
  
  protected abstract Class<?>[] getValidationClasses();
  
  protected abstract void appendExpectedEntries(ExpectedEntryAppender expectedEntries);
  
  private static Set<String> scanClassesForPropertyNames(Class<?>... classes) {
    return Arrays.stream(classes)
        .map(Class::getFields)
        .flatMap(Arrays::stream)
        .filter(AbstractClientConfig::isFieldConstant)
        .filter(AbstractClientConfig::isFieldStringType)
        .filter(not(AbstractClientConfig::isFieldDoc))
        .map(AbstractClientConfig::retrieveField)
        .collect(Collectors.toSet());
  }
  
  private static boolean isFieldConstant(Field field) {
    return Modifier.isFinal(field.getModifiers()) && Modifier.isStatic(field.getModifiers());
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
