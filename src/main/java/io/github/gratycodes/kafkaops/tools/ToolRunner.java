package io.github.gratycodes.kafkaops.tools;

import io.github.gratycodes.kafkaops.audit.AuditEvent;
import io.github.gratycodes.kafkaops.audit.AuditSink;
import io.github.gratycodes.kafkaops.policy.PolicyViolationException;
import io.github.gratycodes.kafkaops.policy.Scope;
import io.github.gratycodes.kafkaops.policy.ToolPolicy;
import io.github.gratycodes.kafkaops.redact.Redactor;
import io.github.gratycodes.kafkaops.telemetry.ToolTracer;
import io.opentelemetry.api.trace.Span;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tools.jackson.databind.json.JsonMapper;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.function.Supplier;

/**
 * The single path every tool call goes through: span, scope check, execution, audit. Keeping this in one place
 * means a new tool cannot forget to audit itself or skip its scope check.
 */
public class ToolRunner {

    private static final Logger log = LoggerFactory.getLogger(ToolRunner.class);

    private final ToolPolicy policy;
    private final AuditSink audit;
    private final ToolTracer tracer;
    private final Redactor redactor;
    private final JsonMapper json;
    private final Clock clock;
    private final boolean captureArguments;

    public ToolRunner(ToolPolicy policy, AuditSink audit, ToolTracer tracer, Redactor redactor, JsonMapper json,
                      Clock clock, boolean captureArguments) {
        this.policy = policy;
        this.audit = audit;
        this.tracer = tracer;
        this.redactor = redactor;
        this.json = json;
        this.clock = clock;
        this.captureArguments = captureArguments;
    }

    /** Thrown back to the MCP layer, which turns it into a tool result with {@code isError=true}. */
    public static class ToolFailure extends RuntimeException {
        private final String reason;

        public ToolFailure(String reason, String message, Throwable cause) {
            super(message, cause);
            this.reason = reason;
        }

        public String reason() {
            return reason;
        }
    }

    public <T extends Summarized> T run(String tool, String description, Scope scope, Map<String, Object> args,
                                        Caller caller, Supplier<T> body) {
        Instant start = clock.instant();
        Map<String, Object> safeArgs = redactor.arguments(args);
        Span span = tracer.start(tool, description, caller.sessionId());
        if (captureArguments) {
            span.setAttribute(ToolTracer.TOOL_CALL_ARGUMENTS, json.writeValueAsString(safeArgs));
        }
        String traceId = span.getSpanContext().isValid() ? span.getSpanContext().getTraceId() : null;
        try (io.opentelemetry.context.Scope ignored = span.makeCurrent()) {
            policy.requireScope(scope);
            if (scope.isWrite()) {
                // Write-ahead audit: if this line cannot be written, the write does not happen.
                audit.record(new AuditEvent(clock.instant(), caller.principal(), caller.client(), caller.sessionId(),
                        tool, scope.id(), safeArgs, AuditEvent.Outcome.ATTEMPT, null, "write requested", 0, traceId));
            }
            T result = body.get();
            span.setAttribute(ToolTracer.OUTCOME, "ok");
            write(start, caller, tool, scope, safeArgs, AuditEvent.Outcome.OK, null, result.auditSummary(), traceId);
            return result;
        } catch (PolicyViolationException e) {
            ToolTracer.fail(span, e.reason(), "denied", e.getMessage());
            write(start, caller, tool, scope, safeArgs, AuditEvent.Outcome.DENIED, e.reason(), e.getMessage(), traceId);
            throw new ToolFailure(e.reason(), "Denied: " + e.getMessage(), e);
        } catch (IllegalArgumentException e) {
            ToolTracer.fail(span, "invalid_argument", "error", e.getMessage());
            write(start, caller, tool, scope, safeArgs, AuditEvent.Outcome.ERROR, "invalid_argument", e.getMessage(), traceId);
            throw new ToolFailure("invalid_argument", "Invalid argument: " + e.getMessage(), e);
        } catch (RuntimeException e) {
            String type = e.getClass().getName();
            String message = redactor.text(String.valueOf(e.getMessage())).text();
            log.warn("Tool {} failed: {}", tool, message);
            ToolTracer.fail(span, type, "error", message);
            write(start, caller, tool, scope, safeArgs, AuditEvent.Outcome.ERROR, type, message, traceId);
            throw new ToolFailure(type, "Tool failed: " + message, e);
        } finally {
            span.end();
        }
    }

    private void write(Instant start, Caller caller, String tool, Scope scope, Map<String, Object> args,
                       AuditEvent.Outcome outcome, String reason, String summary, String traceId) {
        long ms = Duration.between(start, clock.instant()).toMillis();
        try {
            audit.record(new AuditEvent(clock.instant(), caller.principal(), caller.client(), caller.sessionId(), tool,
                    scope.id(), args, outcome, reason, summary, ms, traceId));
        } catch (RuntimeException e) {
            // The call has already happened; failing it now would only hide the result. Writes are covered by
            // the write-ahead ATTEMPT event above.
            log.error("Failed to write audit event for {}: {}", tool, e.toString());
        }
    }
}
