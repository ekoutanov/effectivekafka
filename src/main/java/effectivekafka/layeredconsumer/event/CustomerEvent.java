package effectivekafka.layeredconsumer.event;

import java.util.*;

import com.fasterxml.jackson.annotation.*;

@JsonTypeInfo(use=JsonTypeInfo.Id.NAME, include=JsonTypeInfo.As.EXISTING_PROPERTY, property="type")
@JsonSubTypes({
  @JsonSubTypes.Type(value=CreateCustomer.class, name=CreateCustomer.TYPE),
  @JsonSubTypes.Type(value=UpdateCustomer.class, name=UpdateCustomer.TYPE),
  @JsonSubTypes.Type(value=SuspendCustomer.class, name=SuspendCustomer.TYPE),
  @JsonSubTypes.Type(value=ReinstateCustomer.class, name=ReinstateCustomer.TYPE)
})
public abstract class CustomerEvent {
  @JsonProperty
  private final UUID id;
  
  CustomerEvent(UUID id) {
    this.id = id;
  }
  
  public abstract String getType();
  
  public final UUID getId() {
    return id;
  }
  
  protected final String baseToString() {
    return "id=" + id;
  }
}
