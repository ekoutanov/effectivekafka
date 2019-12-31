package effectivekafka.customerevents.sender;

import java.util.*;

import effectivekafka.customerevents.event.*;
import effectivekafka.customerevents.sender.EventSender.*;

public final class ProducerBusinessLogic {
  private final EventSender sender;
  
  public ProducerBusinessLogic(EventSender sender) {
    this.sender = sender;
  }
  
  public void generateRandomEvents() throws SendException, InterruptedException {
    final var create = new CreateCustomer(UUID.randomUUID(), "Bob", "Brown");
    sender.blockingSend(create);
    
    if (Math.random() > 0.5) {
      final var update = new UpdateCustomer(create.getId(), "Charlie", "Brown");
      sender.blockingSend(update);
    }
    
    if (Math.random() > 0.5) {
      final var suspend = new SuspendCustomer(create.getId());
      sender.blockingSend(suspend);
      
      if (Math.random() > 0.5) {
        final var reinstate = new ReinstateCustomer(create.getId());
        sender.blockingSend(reinstate);
      }
    }
  }
}
