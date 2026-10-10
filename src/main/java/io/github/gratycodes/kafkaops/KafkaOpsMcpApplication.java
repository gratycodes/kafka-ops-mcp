package io.github.gratycodes.kafkaops;

import io.github.gratycodes.kafkaops.config.KafkaOpsProperties;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

@SpringBootApplication
@EnableConfigurationProperties(KafkaOpsProperties.class)
public class KafkaOpsMcpApplication {

    public static void main(String[] args) {
        SpringApplication.run(KafkaOpsMcpApplication.class, args);
    }
}
