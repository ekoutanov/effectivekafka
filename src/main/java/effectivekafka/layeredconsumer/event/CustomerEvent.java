package effectivekafka.layeredconsumer.event;

import java.util.*;

import com.fasterxml.jackson.annotation.*;

@JsonTypeInfo(use=JsonTypeInfo.Id.NAME, include=JsonTypeInfo.As.EXISTING_PROPERTY, property="type")
@JsonSubTypes({
  @JsonSubTypes.Type(value=CreateCustomerEvent.class, name=CreateCustomerEvent.TYPE) 
})
public abstract class CustomerEvent {
  @JsonProperty
  private final UUID id;
  
  CustomerEvent(UUID id) {
    this.id = id;
  }
  
  public abstract String getType();
  
  public UUID getId() {
    return id;
  }
  
  protected final String baseToString() {
    return "id=" + id;
  }
}
