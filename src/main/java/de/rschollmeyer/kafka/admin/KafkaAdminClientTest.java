package de.rschollmeyer.kafka.admin;

import de.rschollmeyer.kafka.configuration.KafkaConfiguration;
import de.rschollmeyer.kafka.consumer.KafkaConsumerTest;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.config.SaslConfigs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Properties;

public class KafkaAdminClientTest {

    private Logger log = LoggerFactory.getLogger(KafkaConsumerTest.class);

    private AdminClient adminClient;

    private KafkaConfiguration kafkaConfiguration;

    public KafkaAdminClientTest(KafkaConfiguration kafkaConfiguration) {
        this.kafkaConfiguration = kafkaConfiguration;

        String brokers = String.join(",", kafkaConfiguration.getHosts());
        String adminUsername = kafkaConfiguration.getAdminUsername();
        String adminPassword = kafkaConfiguration.getAdminPassword();

        Properties properties = new Properties();

        properties.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        properties.setProperty(AdminClientConfig.SECURITY_PROTOCOL_CONFIG, "SASL_SSL");

        properties.setProperty(SaslConfigs.SASL_MECHANISM, "SCRAM-SHA-256");
        properties.setProperty(SaslConfigs.SASL_JAAS_CONFIG,
                "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"" + adminUsername + "\" password=\"" + adminPassword + "\";");

        this.adminClient = KafkaAdminClient.create(properties);
    }

    public void deleteTopic() {
        DeleteTopicsResult deleteTopicsResult = adminClient
                .deleteTopics(Arrays.asList(kafkaConfiguration.getTopicName()));

        while (!deleteTopicsResult.all().isDone()) {}

        log.info("Deleted Topics: " + kafkaConfiguration.getTopicName());
    }

    public void createTopic() {
       CreateTopicsResult result = adminClient.createTopics(Arrays.asList(
          new NewTopic(kafkaConfiguration.getTopicName(),
                  kafkaConfiguration.getPartitions(),
                  kafkaConfiguration.getReplicas())
       ));

       while (!result.all().isDone()) {}

       log.info("Created Topic: " + kafkaConfiguration.getTopicName());
    }
}
