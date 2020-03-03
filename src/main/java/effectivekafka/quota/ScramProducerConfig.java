package effectivekafka.quota;

import java.util.*;

import org.apache.kafka.clients.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.config.*;
import org.apache.kafka.common.security.scram.*;
import org.apache.kafka.common.serialization.*;

import effectivekafka.framework.config.*;

public final class ScramProducerConfig extends AbstractClientConfig<ScramProducerConfig> {
  private String bootstrapServers;
  
  private String username;
  
  private String password;
  
  private String clientId = "";

  public ScramProducerConfig withBootstrapServers(String bootstrapServers) {
    this.bootstrapServers = bootstrapServers;
    return this;
  }
  
  public ScramProducerConfig withUsername(String username) {
    this.username = username;
    return this;
  }

  public ScramProducerConfig withPassword(String password) {
    this.password = password;
    return this;
  }

  public ScramProducerConfig withClientId(String clientId) {
    this.clientId = clientId;
    return this;
  }

  @Override
  protected Class<?>[] getValidationClasses() {
    return new Class<?>[] {
      CommonClientConfigs.class,
      ProducerConfig.class,
      SecurityConfig.class, 
      SaslConfigs.class,
      SslConfigs.class
    };
  }

  @Override
  protected void appendExpectedEntries(ExpectedEntryAppender expectedEntries) {
    expectedEntries.append(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    expectedEntries.append(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    expectedEntries.append(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_SSL");
    expectedEntries.append(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, "https");
    expectedEntries.append(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, "client.truststore.jks");
    expectedEntries.append(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, "secret");
    expectedEntries.append(SaslConfigs.SASL_MECHANISM, "SCRAM-SHA-512");
    
    Objects.requireNonNull(bootstrapServers, "Bootstrap servers not set");
    expectedEntries.append(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    
    Objects.requireNonNull(username, "Username not set");
    Objects.requireNonNull(password, "Password not set");
    final var loginModuleClass = ScramLoginModule.class.getName();
    final var saslJaasConfig = loginModuleClass 
        + " required\n"
        + "username=\"" + username + "\"\n"
        + "password=\""+ password + "\";";

    expectedEntries.append(SaslConfigs.SASL_JAAS_CONFIG, saslJaasConfig);
    
    Objects.requireNonNull(clientId, "Client ID not set");
    expectedEntries.append(ProducerConfig.CLIENT_ID_CONFIG, clientId);
  }
}
