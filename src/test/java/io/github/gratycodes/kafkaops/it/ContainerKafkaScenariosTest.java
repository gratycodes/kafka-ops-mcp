package io.github.gratycodes.kafkaops.it;

import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;

/**
 * Same scenarios against a real Apache Kafka broker in Docker. Skipped automatically when Docker is not
 * available (for example on a laptop without it); runs in GitHub Actions.
 */
@Testcontainers(disabledWithoutDocker = true)
class ContainerKafkaScenariosTest extends KafkaOpsScenarios {

    @Container
    static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka:4.2.1");

    @Override
    protected String bootstrap() {
        return KAFKA.getBootstrapServers();
    }
}
