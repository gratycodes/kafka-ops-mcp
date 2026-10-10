package io.github.gratycodes.kafkaops.audit;

/** Where audit events go. The default writes JSON lines through a dedicated logger. */
@FunctionalInterface
public interface AuditSink {

    void record(AuditEvent event);
}
