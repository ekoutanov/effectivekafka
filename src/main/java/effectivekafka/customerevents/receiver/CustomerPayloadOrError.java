package effectivekafka.customerevents.receiver;

import effectivekafka.customerevents.event.*;

public final class CustomerPayloadOrError {
  private final CustomerPayload payload;
  
  private final Throwable error;
  
  private String encodedValue;

  public CustomerPayloadOrError(CustomerPayload payload, Throwable error, String encodedValue) {
    this.payload = payload;
    this.error = error;
    this.encodedValue = encodedValue;
  }

  public CustomerPayload getPayload() {
    return payload;
  }

  public Throwable getError() {
    return error;
  }
  
  public String getEncodedValue() {
    return encodedValue;
  }

  @Override
  public String toString() {
    return CustomerPayloadOrError.class.getSimpleName() + " [payload=" + payload + 
        ", error=" + error + ", encodedValue=" + encodedValue + "]";
  }
}
