package io.github.gratycodes.kafkaops.audit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tools.jackson.databind.json.JsonMapper;

/**
 * Writes each event as one JSON object to the {@code kafka_ops.audit} logger. logback-spring.xml routes that
 * logger to its own file ({@code KAFKA_OPS_AUDIT_FILE}) so it can be shipped separately from app logs.
 */
public class JsonLogAuditSink implements AuditSink {

    private static final Logger AUDIT = LoggerFactory.getLogger("kafka_ops.audit");

    private final JsonMapper json;

    public JsonLogAuditSink(JsonMapper json) {
        this.json = json;
    }

    @Override
    public void record(AuditEvent event) {
        AUDIT.info(json.writeValueAsString(event));
    }
}
