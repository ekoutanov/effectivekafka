package effectivekafka.customerevents.event;

import java.util.*;

import com.fasterxml.jackson.annotation.*;

public final class UpdateCustomer extends CustomerPayload {
  static final String TYPE = "UPDATE_CUSTOMER";

  @JsonProperty
  private final String firstName;

  @JsonProperty
  private final String lastName;

  public UpdateCustomer(@JsonProperty("id") UUID id, 
                        @JsonProperty("firstName") String firstName, 
                        @JsonProperty("lastName") String lastName) {
    super(id);
    this.firstName = firstName;
    this.lastName = lastName;
  }

  @Override
  public String getType() {
    return TYPE;
  }

  public String getFirstName() {
    return firstName;
  }

  public String getLastName() {
    return lastName;
  }

  @Override
  public String toString() {
    return UpdateCustomer.class.getSimpleName() + " [" + baseToString() + 
        ", firstName=" + firstName + ", lastName=" + lastName + "]";
  }
}
