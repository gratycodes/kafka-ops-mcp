package io.github.gratycodes.kafkaops.support;

import io.github.gratycodes.kafkaops.audit.AuditEvent;
import io.github.gratycodes.kafkaops.audit.AuditSink;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

public class RecordingAuditSink implements AuditSink {

    public final List<AuditEvent> events = new CopyOnWriteArrayList<>();

    @Override
    public void record(AuditEvent event) {
        events.add(event);
    }

    public AuditEvent last() {
        return events.getLast();
    }
}
