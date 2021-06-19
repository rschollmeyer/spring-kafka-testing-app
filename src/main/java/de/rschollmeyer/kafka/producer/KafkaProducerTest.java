package de.rschollmeyer.kafka.producer;

import de.rschollmeyer.kafka.configuration.KafkaConfiguration;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class KafkaProducerTest implements Runnable {

    private Logger log = LoggerFactory.getLogger(KafkaProducerTest.class);

    private Producer<String, String> producer;

    private KafkaConfiguration kafkaConfiguration;

    public KafkaProducerTest(KafkaConfiguration kafkaConfiguration) {
        this.kafkaConfiguration = kafkaConfiguration;

        Properties properties = new Properties();

        String brokers = String.join(",", kafkaConfiguration.getHosts());

        // necessary config
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // additional config
        properties.setProperty(ProducerConfig.ACKS_CONFIG, ""+kafkaConfiguration.getAcks());

        if(kafkaConfiguration.isSecurityEnabled()) {
            String username = kafkaConfiguration.getUsername();
            String password = kafkaConfiguration.getPassword();

            properties.setProperty(AdminClientConfig.SECURITY_PROTOCOL_CONFIG, "SASL_SSL");
            properties.setProperty(SaslConfigs.SASL_MECHANISM, "SCRAM-SHA-256");
            properties.setProperty(SaslConfigs.SASL_JAAS_CONFIG,
                    "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"" + username + "\" password=\"" + password + "\";");
        }

        this.producer = new KafkaProducer<>(properties);
    }

    public void produce() throws ExecutionException, InterruptedException {
        int counter = 0;

        while(true) {
            String item = UUID.randomUUID().toString();

            ProducerRecord<String, String> producerRecord =
                    new ProducerRecord<>(kafkaConfiguration.getTopicName(), item);
            Future<RecordMetadata> record = producer.send(producerRecord);

            log.info("Produced: " + ++counter +
                    ", Partition: " + record.get().partition() +
                    ", Offset: " + record.get().offset() +
                    ", Value: " + item);

            try {
                Thread.sleep(kafkaConfiguration.getSleepTimeMs());
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public void run() {
        try {
            produce();
        } catch (ExecutionException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
