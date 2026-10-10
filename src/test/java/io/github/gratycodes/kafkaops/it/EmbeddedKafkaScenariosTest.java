package io.github.gratycodes.kafkaops.it;

import org.springframework.kafka.test.condition.EmbeddedKafkaCondition;
import org.springframework.kafka.test.context.EmbeddedKafka;

/** Runs every scenario against an in-JVM KRaft broker. No Docker needed. */
@EmbeddedKafka(partitions = 1)
class EmbeddedKafkaScenariosTest extends KafkaOpsScenarios {

    @Override
    protected String bootstrap() {
        return EmbeddedKafkaCondition.getBroker().getBrokersAsString();
    }
}
