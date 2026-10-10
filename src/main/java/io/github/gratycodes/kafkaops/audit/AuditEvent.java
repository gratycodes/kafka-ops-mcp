package io.github.gratycodes.kafkaops.audit;

import java.time.Instant;
import java.util.Map;

/**
 * One line of the audit log. Arguments are already redacted; results are summarised, never copied,
 * so the audit log does not become a second copy of the DLQ.
 */
public record AuditEvent(
        Instant timestamp,
        String principal,
        String client,
        String sessionId,
        String tool,
        String scope,
        Map<String, Object> args,
        Outcome outcome,
        String reason,
        String summary,
        long durationMs,
        String traceId) {

    /** ATTEMPT is written before any write so a crash mid-replay still leaves a trace. */
    public enum Outcome { ATTEMPT, OK, DENIED, ERROR }
}
