package de.rschollmeyer.kafka;

import de.rschollmeyer.kafka.admin.KafkaAdminClientTest;
import de.rschollmeyer.kafka.configuration.KafkaConfiguration;
import de.rschollmeyer.kafka.consumer.KafkaConsumerTest;
import de.rschollmeyer.kafka.producer.KafkaProducerTest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class Application {

    private static Logger log = LoggerFactory.getLogger(Application.class);

    private static KafkaConfiguration kafkaConfiguration;

    public Application(KafkaConfiguration kafkaConfiguration) {
        this.kafkaConfiguration = kafkaConfiguration;
    }

    public static void main(String[] args) {
        SpringApplication.run(Application.class, args);

        KafkaAdminClientTest adminClient = new KafkaAdminClientTest(kafkaConfiguration);

        adminClient.createTopic();

        log.info("Start producing messages...");

        Thread prod = new Thread(new KafkaProducerTest(kafkaConfiguration));
        prod.start();

        log.info("Start consuming messages...");

        Thread cons = new Thread(new KafkaConsumerTest(kafkaConfiguration));
        cons.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> adminClient.deleteTopic()));
    }
}
