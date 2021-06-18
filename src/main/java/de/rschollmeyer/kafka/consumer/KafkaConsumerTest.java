package de.rschollmeyer.kafka.consumer;

import de.rschollmeyer.kafka.configuration.KafkaConfiguration;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class KafkaConsumerTest implements Runnable {

    private Logger log = LoggerFactory.getLogger(KafkaConsumerTest.class);

    KafkaConsumer<String, String> kafkaConsumer;

    private KafkaConfiguration kafkaConfiguration;

    public KafkaConsumerTest(KafkaConfiguration kafkaConfiguration) {
        this.kafkaConfiguration = kafkaConfiguration;

        String brokers = String.join(",", kafkaConfiguration.getHosts());
        String username = kafkaConfiguration.getUsername();
        String password = kafkaConfiguration.getPassword();

        Properties properties = new Properties();

        // necessary config
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        properties.setProperty(AdminClientConfig.SECURITY_PROTOCOL_CONFIG, "SASL_SSL");
        properties.setProperty(SaslConfigs.SASL_MECHANISM, "SCRAM-SHA-256");
        properties.setProperty(SaslConfigs.SASL_JAAS_CONFIG,
                "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"" + username + "\" password=\"" + password + "\";");

        // additional config
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, kafkaConfiguration.getConsumerGroupId());
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        this.kafkaConsumer = new KafkaConsumer<>(properties);
        this.kafkaConsumer.subscribe(Arrays.asList(kafkaConfiguration.getTopicName()));
    }

    public void consume() {
        int counter = 0;

        while(true) {

            ConsumerRecords<String, String> consumerRecords =
                    this.kafkaConsumer.poll(Duration.ofMillis(5));

            for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {

                log.info("Consumed: " + ++counter +
                        ", Partition: " + consumerRecord.partition() +
                        ", Offset: " + consumerRecord.offset() +
                        ", Value: " + consumerRecord.value());
            }

            this.kafkaConsumer.commitSync();
        }
    }

    @Override
    public void run() {
        consume();
    }
}
